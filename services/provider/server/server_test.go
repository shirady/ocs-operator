package server

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	nbv1 "github.com/noobaa/noobaa-operator/v5/pkg/apis/noobaa/v1alpha1"
	ocsv1a1 "github.com/red-hat-storage/ocs-operator/api/v4/v1alpha1"
	pb "github.com/red-hat-storage/ocs-operator/services/provider/api/v4"
	"github.com/red-hat-storage/ocs-operator/v4/controllers/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReplaceMsgr1PortWithMsgr2(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "no msgr1 port",
			input:    []string{"192.168.1.1:3300", "192.168.1.2:3300", "192.168.1.3:3300"},
			expected: []string{"192.168.1.1:3300", "192.168.1.2:3300", "192.168.1.3:3300"},
		},
		{
			name:     "all msgr1 ports",
			input:    []string{"192.168.1.1:6789", "192.168.1.2:6789", "192.168.1.3:6789"},
			expected: []string{"192.168.1.1:3300", "192.168.1.2:3300", "192.168.1.3:3300"},
		},
		{
			name:     "mixed ports",
			input:    []string{"192.168.1.1:6789", "192.168.1.2:3300", "192.168.1.3:6789"},
			expected: []string{"192.168.1.1:3300", "192.168.1.2:3300", "192.168.1.3:3300"},
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "no port in IP",
			input:    []string{"192.168.1.1", "192.168.1.2:6789", "192.168.1.2:6789"},
			expected: []string{"192.168.1.1", "192.168.1.2:3300", "192.168.1.2:3300"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of the input slice to avoid modifying the original
			inputCopy := make([]string, len(tt.input))
			copy(inputCopy, tt.input)
			replaceMsgr1PortWithMsgr2(inputCopy)
			if !reflect.DeepEqual(inputCopy, tt.expected) {
				t.Errorf("replaceMsgr1PortWithMsgr2() = %v, expected %v", inputCopy, tt.expected)
			}
		})
	}
}

func TestGetKubeResourcesForClass(t *testing.T) {
	srcClassName := "class-a"

	srcSc := &storagev1.StorageClass{}
	srcSc.Name = srcClassName
	srcSc.Parameters = map[string]string{
		"key1": "val1",
		"keyn": "valn",
	}
	srcSc.Provisioner = "whoami"
	srcSc.MountOptions = []string{"mount", "secretly"}

	consumer := &ocsv1a1.StorageConsumer{}
	classItem := ocsv1a1.StorageClassSpec{}
	classItem.Name = srcClassName
	classItem.Aliases = append(classItem.Aliases, "class-1", "class-2")
	consumer.Spec.StorageClasses = append(
		consumer.Spec.StorageClasses,
		classItem,
	)
	genClassFn := func(srcName string) (client.Object, error) {
		return srcSc, nil
	}

	objs := getKubeResourcesForClass(
		klog.Background(),
		consumer.Spec.StorageClasses,
		"StorageClass",
		genClassFn,
	)

	// class-a, class-1 and class-2
	wantObjs := 3
	if gotObjs := len(objs); gotObjs != wantObjs {
		t.Fatalf("expected %d objects, got %d", wantObjs, gotObjs)
	}

	objIdxByName := make(map[string]int, len(objs))
	for idx, obj := range objs {
		objIdxByName[obj.GetName()] = idx
	}

	for _, expName := range []string{"class-1", "class-2"} {
		t.Run(expName, func(t *testing.T) {
			wantObj := srcSc.DeepCopy()
			wantObj.Name = expName
			idx, exist := objIdxByName[wantObj.Name]
			if !exist {
				t.Fatalf("expected storageclass with name %s to exist", wantObj.Name)
			}
			gotObj := objs[idx]
			// except the name the whole object should be deep equal
			wantObj.Name = expName
			if !equality.Semantic.DeepEqual(gotObj, wantObj) {
				t.Fatalf("expected %v to be deep equal to %v", gotObj, wantObj)
			}
		})
	}
}

func TestNotify(t *testing.T) {
	ctx := context.Background()
	storageConsumer := &ocsv1a1.StorageConsumer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-consumer",
			Namespace: testNamespace,
			UID:       "client-123",
		},
	}
	obcPayload := map[string]interface{}{
		"name":               "test-obc",
		"namespace":          "app-namespace",
		"storageClassName":   "openshift-storage.noobaa.io",
		"generateBucketName": "test-obc-0402",
	}
	payloadBytes, err := json.Marshal(obcPayload)
	if err != nil {
		t.Fatalf("failed to marshal OBC payload: %v", err)
	}

	tests := []struct {
		name        string
		setupServer func(t *testing.T) *OCSProviderServer
		req         *pb.NotifyRequest
		wantErrCode codes.Code
		validate    func(t *testing.T, srv *OCSProviderServer)
	}{
		{
			name: "unspecified action returns internal",
			setupServer: func(t *testing.T) *OCSProviderServer {
				return &OCSProviderServer{}
			},
			req: &pb.NotifyRequest{
				ClientID: "client-123",
				Event:    pb.Event_OBC_ACTION_UNSPECIFIED,
			},
			wantErrCode: codes.Internal,
		},
		{
			name: "obc create request creates resource",
			setupServer: func(t *testing.T) *OCSProviderServer {
				scheme, schemeErr := newScheme()
				if schemeErr != nil {
					t.Fatalf("newScheme() error = %v", schemeErr)
				}
				fakeClient := fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(storageConsumer).
					WithIndex(&ocsv1a1.StorageConsumer{}, util.ObjectUidIndexName, util.ObjectUidIndexFieldFunc).
					Build()
				return &OCSProviderServer{
					client:          fakeClient,
					consumerManager: createTestConsumerManager(fakeClient),
					namespace:       testNamespace,
				}
			},
			req: &pb.NotifyRequest{
				ClientID: string(storageConsumer.UID),
				Event:    pb.Event_OBC_CREATE,
				Payload:  payloadBytes,
			},
			wantErrCode: codes.OK,
			validate: func(t *testing.T, srv *OCSProviderServer) {
				expectedName := "remote-obc-" + getObcHash(string(storageConsumer.UID), "test-obc", "app-namespace")
				obc := &nbv1.ObjectBucketClaim{}
				if err := srv.client.Get(ctx, types.NamespacedName{
					Name:      expectedName,
					Namespace: testNamespace,
				}, obc); err != nil {
					t.Fatalf("expected OBC to be created: %v", err)
				}

				labelValue := createObcLabelValue(ctx, storageConsumer.Name, "test-obc", "app-namespace")
				if obc.Labels[labelKey] != labelValue {
					t.Fatalf("expected label %s=%s, got %v", labelKey, labelValue, obc.Labels)
				}
				if obc.Annotations[annotationKeyRemoteOBCCreation] != "true" {
					t.Fatalf("expected annotation %s=true, got %v", annotationKeyRemoteOBCCreation, obc.Annotations)
				}
				if obc.Annotations[annotationKeyRemoteOBCOriginalName] != "test-obc" {
					t.Fatalf("expected annotation %s=test-obc, got %v", annotationKeyRemoteOBCOriginalName, obc.Annotations)
				}
				if obc.Annotations[annotationKeyRemoteOBCOriginalNamespace] != "app-namespace" {
					t.Fatalf("expected annotation %s=app-namespace, got %v", annotationKeyRemoteOBCOriginalNamespace, obc.Annotations)
				}

				if obc.Spec.StorageClassName != "openshift-storage.noobaa.io" {
					t.Fatalf("expected storageClassName test-obc-0402, got %q", obc.Spec.StorageClassName)
				}
				if obc.Spec.GenerateBucketName != "test-obc-0402" {
					t.Fatalf("expected generateBucketName test-obc-0402, got %q", obc.Spec.GenerateBucketName)
				}

				if len(obc.OwnerReferences) == 0 {
					t.Fatalf("expected ownerReferences to be set")
				}
				foundOwner := false
				for _, ownerRef := range obc.OwnerReferences {
					if ownerRef.Kind == "StorageConsumer" &&
						ownerRef.Name == storageConsumer.Name &&
						ownerRef.UID == storageConsumer.UID &&
						ownerRef.APIVersion == ocsv1a1.GroupVersion.String() {
						foundOwner = true
						break
					}
				}
				if !foundOwner {
					t.Fatalf("expected StorageConsumer owner reference, got %v", obc.OwnerReferences)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := tt.setupServer(t)
			resp, err := srv.Notify(ctx, tt.req)

			if tt.wantErrCode == codes.OK {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if resp == nil {
					t.Fatalf("expected non-nil response")
				}
			} else {
				if resp != nil {
					t.Fatalf("expected nil response, got %#v", resp)
				}
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if status.Code(err) != tt.wantErrCode {
					t.Fatalf("expected %v error, got %v", tt.wantErrCode, status.Code(err))
				}
			}

			if tt.validate != nil {
				tt.validate(t, srv)
			}
		})
	}
}
