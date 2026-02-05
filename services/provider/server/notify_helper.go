package server

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"

	nbv1 "github.com/noobaa/noobaa-operator/v5/pkg/apis/noobaa/v1alpha1"
	ocsv1alpha1 "github.com/red-hat-storage/ocs-operator/api/v4/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	labelKey                                = "obc-details"
	annotationKeyRemoteOBCCreation          = "remote-obc-creation"
	annotationKeyRemoteOBCOriginalName      = "remote-obc-original-name"
	annotationKeyRemoteOBCOriginalNamespace = "remote-obc-original-namespace"
	prefixOfHashedName                      = "remote-obc"
)

// handleObcCreate create the OBC that the client cluster asked for on the provider cluster.
// It is a synchronous call, we do not wait for resources to be created.
// Notes:
//   - OBC is created in the provider server namespace with an obscure name to avoid collisions.
//   - Label added: key of "obc-details" and the value includes details on the storage consumer and the original OBC details.
//   - Annotations added:
//   - "remote-obc-creation": "true"                         // indicates OBC was created by the notify flow
//   - "remote-obc-original-name": "<original-obc-name>"       // name provided by the client
//   - "remote-obc-original-namespace": "<original-obc-namespace>"    // namespace provided by the client
//   - Owner reference is set to the storage consumer
func (s *OCSProviderServer) handleObcCreate(ctx context.Context, clientID string, payload []byte) error {
	logger := klog.FromContext(ctx).WithName("handleObcCreate")
	logger.Info("handleObcCreate: Starting handleObcCreate", "clientID", clientID)

	// (1) get the storage consumer
	storageConsumerUUID := clientID // SDSD temp
	storageConsumer, err := s.consumerManager.Get(ctx, storageConsumerUUID)
	if err != nil {
		logger.Error(err, "handleObcCreate: failed to get StorageConsumer", "storageConsumerUUID", storageConsumerUUID)
		return status.Errorf(codes.Internal, "OBC cannot be created due to failed StorageConsumer lookup: storageConsumerUUID=%s", storageConsumerUUID)
	}
	if storageConsumer == nil {
		return status.Errorf(codes.Internal, "OBC cannot be created due to missing StorageConsumer: storageConsumerUUID=%s", storageConsumerUUID)
	}
	storageConsumerName := storageConsumer.Name

	// (2) parse the payload to extract OBC details
	var obcDetails map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &obcDetails); err != nil {
		logger.Error(err, "handleObcCreate: Failed to unmarshal payload")
		return status.Errorf(codes.Internal, "Failed to unmarshal payload: %v", err)
	}
	// extract name and namespace
	obcNameClient, err := getRequiredStringField(obcDetails, "name")
	if err != nil {
		logger.Error(err, "handleObcCreate: Failed to extract OBC name from payload")
		return status.Errorf(codes.InvalidArgument, "Failed to extract OBC name from payload: %v", err)
	}
	namespaceNameClient, err := getRequiredStringField(obcDetails, "namespace")
	if err != nil {
		logger.Error(err, "handleObcCreate: Failed to extract OBC namespace from payload")
		return status.Errorf(codes.InvalidArgument, "Failed to extract OBC namespace from payload: %v", err)
	}

	// (3) check that we do not already have an OBC with this name and namespace for a storage consumer
	existingOBC, err := checkIfObcExists(ctx, s.client, s.namespace, storageConsumerName, obcNameClient, namespaceNameClient)
	if err != nil {
		logger.Error(err, "handleObcCreate: OBC existence check failed")
		return status.Errorf(codes.Internal, "Failed to check OBC existence check: %v", err)
	}
	if existingOBC != nil {
		logger.Error(err, "handleObcCreate: OBC already exists", "existing OBC Name", existingOBC.GetName(), "existing OBC Namespace", existingOBC.GetNamespace())
		return status.Errorf(codes.AlreadyExists, "Failed to create OBC since it already exists in namespace %s with name %s", namespaceNameClient, obcNameClient)
	}
	logger.Info("handleObcCreate: OBC does not exist, proceeding with creation", "clientID", clientID, "obcNameClient", obcNameClient, "namespaceNameClient", namespaceNameClient)

	// (4) build OBC object
	obc := &nbv1.ObjectBucketClaim{}
	// set resource name to a hash based on storage consumer UUID, OBC name, and OBC namespace
	obc.Name = getObcHashedName(storageConsumerUUID, obcNameClient, namespaceNameClient)
	// create in the namespace associated with the provider server
	obc.Namespace = s.namespace

	// add label for the searching of OBC
	labelValue := createObcLabelValue(ctx, storageConsumerName, obcNameClient, namespaceNameClient)
	if obc.Labels == nil {
		obc.Labels = map[string]string{}
	}
	obc.Labels[labelKey] = labelValue

	// add annotations indicating remote OBC creation and store original details
	if obc.Annotations == nil {
		obc.Annotations = map[string]string{}
	}
	obc.Annotations[annotationKeyRemoteOBCCreation] = "true"                       // used in mcg-cli
	obc.Annotations[annotationKeyRemoteOBCOriginalName] = obcNameClient            // for human use only
	obc.Annotations[annotationKeyRemoteOBCOriginalNamespace] = namespaceNameClient // for human use only

	// populate fields in the OBC spec from payload if present
	if storageClassName, ok := obcDetails["storageClassName"].(string); ok {
		obc.Spec.StorageClassName = storageClassName
	}
	if bucketName, ok := obcDetails["bucketName"].(string); ok {
		obc.Spec.BucketName = bucketName
	}
	if generateBucketName, ok := obcDetails["generateBucketName"].(string); ok {
		obc.Spec.GenerateBucketName = generateBucketName
	}
	obc.Spec.AdditionalConfig = parseAdditionalConfig(obcDetails)

	// set owner reference based on storage consumer
	ownerReference := metav1.OwnerReference{
		APIVersion: ocsv1alpha1.GroupVersion.String(),
		Kind:       "StorageConsumer",
		Name:       storageConsumer.Name,
		UID:        storageConsumer.UID,
		Controller: ptr.To(false),
	}
	obc.OwnerReferences = append(obc.OwnerReferences, ownerReference)
	logger.Info("handleObcCreate: Added owner reference to OBC", "owner", storageConsumer.Name)

	// (5) create the OBC resource
	logger.Info("handleObcCreate: Creating OBC resource", "name", obc.Name, "namespace", obc.Namespace)
	if err := s.client.Create(ctx, obc); err != nil {
		logger.Error(err, "handleObcCreate: Failed to create OBC resource:", "storageConsumerUUID", storageConsumerUUID, "storageConsumerName", storageConsumerName, "name", obc.Name, "namespace", obc.Namespace)
		return status.Errorf(codes.Internal, "failed to create OBC name %s namespace %s: %v", obcNameClient, namespaceNameClient, err)
	}
	logger.Info("handleObcCreate: Successfully created OBC resource", "name", obc.Name, "namespace", obc.Namespace)
	return nil
}

// handleObcDelete delete the OBC that the client cluster asked for on the provider cluster.
// It is a synchronous call, we do not wait for resources to be deleted.
// Notes:
//   - OBC is deleted from the provider server namespace.
func (s *OCSProviderServer) handleObcDelete(ctx context.Context, clientID string, payload []byte) error {
	logger := klog.FromContext(ctx).WithName("handleObcDelete")
	logger.Info("handleObcDelete: Starting handleObcDelete", "clientID", clientID)

	storageConsumerUUID := clientID // SDSD temp

	// (1) parse the payload to extract OBC details
	var obcDetails map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &obcDetails); err != nil {
		logger.Error(err, "handleObcDelete: Failed to unmarshal payload")
		return status.Errorf(codes.Internal, "Failed to unmarshal payload: %v", err)
	}
	// extract name and namespace
	obcNameClient, err := getRequiredStringField(obcDetails, "name")
	if err != nil {
		logger.Error(err, "handleObcDelete: Failed to extract OBC name from payload")
		return status.Errorf(codes.InvalidArgument, "Failed to extract OBC name from payload: %v", err)
	}
	namespaceNameClient, err := getRequiredStringField(obcDetails, "namespace")
	if err != nil {
		logger.Error(err, "handleObcDelete: Failed to extract OBC namespace from payload")
		return status.Errorf(codes.InvalidArgument, "Failed to extract OBC namespace from payload: %v", err)
	}
	// (2) get the hashed name of the OBC
	obcHashedName := getObcHashedName(storageConsumerUUID, obcNameClient, namespaceNameClient)

	// (3) delete the OBC
	logger.Info("handleObcDelete: Deleting OBC resource", "name", obcHashedName, "namespace", s.namespace)
	obc := &nbv1.ObjectBucketClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      obcHashedName,
			Namespace: s.namespace,
		},
	}
	if err := s.client.Delete(ctx, obc); err != nil {
		if client.IgnoreNotFound(err) == nil {
			logger.Info("handleObcDelete: OBC not found", "name", obcHashedName, "namespace", s.namespace)
			return status.Errorf(codes.NotFound, "OBC not found name %s namespace %s", obcNameClient, namespaceNameClient)
		}
		logger.Error(err, "handleObcDelete: Failed to delete OBC resource", "name", obcHashedName, "namespace", s.namespace)
		return status.Errorf(codes.Internal, "failed to delete OBC name %s namespace %s: %v", obcNameClient, namespaceNameClient, err)
	}
	logger.Info("handleObcDelete: Successfully deleted OBC resource", "name", obcHashedName, "namespace", s.namespace)
	return nil
}

// createObcLabelValue create the label value which will include:
// 1. storage consumer name
// 2. original obc name
// 3. original obc namespace
// between each of them a delimiter will be used to separate the values
func createObcLabelValue(ctx context.Context, storageConsumerName string, ObcName string, ObcNamespace string) string {
	logger := klog.FromContext(ctx).WithName("createObcLabelValue")
	delimiter := '_'
	labelValue := fmt.Sprintf("%s%c%s%c%s", storageConsumerName, delimiter, ObcName, delimiter, ObcNamespace)
	logger.Info("createObcLabelValue: Created label", "label", labelValue)
	return labelValue
}

// getObcHash creates a stable hash for naming the created OBC resource.
// this function is based on getStorageRequestHash function
func getObcHash(storageConsumerUUID, obcName, obcNamespace string) string {
	s := struct {
		StorageConsumerUUID string `json:"storageConsumerUUID"`
		ObcName             string `json:"obcName"`
		ObcNamespace        string `json:"obcNamespace"`
	}{
		storageConsumerUUID,
		obcName,
		obcNamespace,
	}
	obcHash, err := json.Marshal(s)
	if err != nil {
		panic("failed to marshal obc hash payload")
	}
	md5Sum := md5.Sum(obcHash)
	return hex.EncodeToString(md5Sum[:16])
}

// getObcHashedName creates a stable hash for OBC name
// obcName and obcNamespace are from the client cluster
func getObcHashedName(storageConsumerUUID, obcName, obcNamespace string) string {
	hash := getObcHash(storageConsumerUUID, obcName, obcNamespace)
	return fmt.Sprintf("%s-%s", prefixOfHashedName, hash)
}

// getRequiredStringField extracts a required string field from the decoded payload map
// returns an error if the key is missing or not a string.
func getRequiredStringField(obcDetails map[string]interface{}, key string) (string, error) {
	if v, ok := obcDetails[key].(string); ok {
		return v, nil
	}
	return "", fmt.Errorf("missing or invalid required field %s", key)
}

// parseAdditionalConfig safely converts a map[string]interface{} to map[string]string by keeping only string values
func parseAdditionalConfig(obcDetails map[string]interface{}) map[string]string {
	res := map[string]string{}
	if ac, ok := obcDetails["additionalConfig"].(map[string]interface{}); ok {
		for k, v := range ac {
			if vs, ok := v.(string); ok {
				res[k] = vs
			}
		}
	}
	return res
}

// checkIfObcExists checks if an OBC with the given name and namespace already exists
// by searching for OBCs with a specific label constructed from storageConsumer name, OBC name, and namespace.
func checkIfObcExists(ctx context.Context, c client.Client, operatorNamespace string, storageConsumerName string, ObcName string, ObcNamespace string) (*nbv1.ObjectBucketClaim, error) {
	logger := klog.FromContext(ctx).WithName("checkIfObcExists")
	logger.Info("checkIfObcExists: Starting checkIfObcExists", "name", ObcName, "namespace", ObcNamespace)

	labelValue := createObcLabelValue(ctx, storageConsumerName, ObcName, ObcNamespace)
	logger.Info("CheckIfObcExists: Searching OBCs by label", "labelKey", labelKey, "labelValue", labelValue)

	// list OBCs filtered by label and operator namespace; limit to 1 item.
	obcList := &nbv1.ObjectBucketClaimList{}
	listOpts := []client.ListOption{
		client.InNamespace(operatorNamespace),
		client.MatchingLabels(map[string]string{labelKey: labelValue}),
		client.Limit(1),
	}
	if err := c.List(ctx, obcList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list OBCs in namespace %s with label %s=%s: %v", operatorNamespace, labelKey, labelValue, err)
	}
	if len(obcList.Items) == 0 {
		logger.Info("No OBC found in namespace matching label", "labelValue", labelValue)
		return nil, nil
	}
	logger.Info("Found OBC", "name", obcList.Items[0].GetName(), "namespace", obcList.Items[0].GetNamespace())
	return &obcList.Items[0], nil
}
