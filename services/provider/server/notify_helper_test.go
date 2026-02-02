package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	nbv1 "github.com/noobaa/noobaa-operator/v5/pkg/apis/noobaa/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// test createObcLabelValue function
func TestCreateObcLabelValue(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                string
		storageConsumerName string
		namespace           string
		obc                 string
		expected            string
	}{
		{
			name:                "basic",
			storageConsumerName: "consumer123",
			obc:                 "myobc",
			namespace:           "ns",
			expected:            "consumer123_myobc_ns",
		},
		{
			name:                "special chars",
			storageConsumerName: "consumer-2",
			obc:                 "obc.name",
			namespace:           "na-me",
			expected:            "consumer-2_obc.name_na-me",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := createObcLabelValue(ctx, tt.storageConsumerName, tt.obc, tt.namespace)
			if got != tt.expected {
				t.Fatalf("createObcLabelValue() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// test getObcHash function
func TestGetObcHash(t *testing.T) {
	tests := []struct {
		name                string
		storageConsumerUUID string
		obcName             string
		obcNamespace        string
		expected            string
	}{
		{
			name:                "basic",
			storageConsumerUUID: "412b006a-8829-4273-82e2-6b3470640717",
			obcName:             "my-obc",
			obcNamespace:        "my-namespace",
			expected:            "95bd6a203873da85f0a2e4467984a5b8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getObcHash(tt.storageConsumerUUID, tt.obcName, tt.obcNamespace)
			if got != tt.expected {
				t.Fatalf("getObcHash() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// getRequiredStringField function
func TestGetRequiredStringField(t *testing.T) {
	tests := []struct {
		name          string
		obcDetails    map[string]interface{}
		key           string
		expected      string
		expectedError error
	}{
		{
			name: "basic name",
			obcDetails: map[string]interface{}{
				"name": "my-obc",
			},
			key:           "name",
			expected:      "my-obc",
			expectedError: nil,
		},
		{
			name: "basic namespace",
			obcDetails: map[string]interface{}{
				"namespace": "my-namespace",
			},
			key:           "namespace",
			expected:      "my-namespace",
			expectedError: nil,
		},
		{
			name: "non-existing key",
			obcDetails: map[string]interface{}{
				"name": "my-obc",
			},
			key:           "non-existing",
			expected:      "",
			expectedError: fmt.Errorf("missing or invalid required field non-existing"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getRequiredStringField(tt.obcDetails, tt.key)
			if tt.expectedError != nil {
				if err == nil || err.Error() != tt.expectedError.Error() {
					t.Fatalf("getRequiredStringField() error = %v, want %v", err, tt.expectedError)
				}
				return
			}
			if err != nil {
				t.Fatalf("getRequiredStringField() unexpected error = %v", err)
			}
			if got != tt.expected {
				t.Fatalf("getRequiredStringField() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// parseAdditionalConfig function
func TestParseAdditionalConfig(t *testing.T) {
	tests := []struct {
		name          string
		obcDetails    map[string]interface{}
		expected      map[string]string
		expectedError error
	}{
		{
			name: "basic",
			obcDetails: map[string]interface{}{
				"additionalConfig": map[string]interface{}{
					"key1": "value1",
					"key2": "value2",
				},
			},
			expected: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expectedError: nil,
		},
		{
			name: "empty additionalConfig",
			obcDetails: map[string]interface{}{
				"additionalConfig": map[string]interface{}{},
			},
			expected:      map[string]string{},
			expectedError: nil,
		},
		{
			name: "non-string value",
			obcDetails: map[string]interface{}{
				"additionalConfig": map[string]interface{}{
					"key1": 123,
				},
			},
			expected:      map[string]string{},
			expectedError: errors.New("value for key key1 is not a string"),
		},
		{
			name: "non-map value",
			obcDetails: map[string]interface{}{
				"additionalConfig": "not a map",
			},
			expected:      map[string]string{},
			expectedError: fmt.Errorf("additionalConfig is not a map"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseAdditionalConfig(tt.obcDetails)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Fatalf("parseAdditionalConfig() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// test checkIfObcExists function
func TestCheckIfObcExists(t *testing.T) {
	ctx := context.Background()
	operatorNamespace := "openshift-storage-test"
	obcHashedName := "obc-hashed-name"
	tests := []struct {
		name                string
		storageConsumerName string
		obcName             string
		obcNamespace        string
		existing            []client.Object
		expected            bool
	}{
		{
			name:                "no obc exists",
			storageConsumerName: "consumer-1",
			obcName:             "obc-1",
			obcNamespace:        "ns-1",
			existing:            []client.Object{},
			expected:            false,
		},
		{
			name:                "obc exists in same namespace",
			storageConsumerName: "consumer-1",
			obcName:             "obc-1",
			obcNamespace:        "ns-1",
			existing: func() []client.Object {
				labelValue := createObcLabelValue(ctx, "consumer-1", "obc-1", "ns-1")
				obc := &nbv1.ObjectBucketClaim{}
				obc.Name = obcHashedName
				obc.Namespace = operatorNamespace
				obc.Labels = map[string]string{labelKey: labelValue}
				return []client.Object{obc}
			}(),
			expected: true,
		},
		{
			name:                "obc exists but different namespace",
			storageConsumerName: "consumer-1",
			obcName:             "obc-1",
			obcNamespace:        "ns-2",
			existing: func() []client.Object {
				obc := &nbv1.ObjectBucketClaim{}
				obc.Name = obcHashedName
				obc.Namespace = "different-ns"
				labelValue := createObcLabelValue(ctx, "consumer-1", "obc-1", obc.Namespace)
				obc.Labels = map[string]string{labelKey: labelValue}
				return []client.Object{obc}
			}(),
			expected: false,
		},
		{
			name:                "different consumers with same name/namespace",
			storageConsumerName: "consumer-2",
			obcName:             "obc-1",
			obcNamespace:        "ns-1",
			existing: func() []client.Object {
				obc := &nbv1.ObjectBucketClaim{}
				obc.Name = "obc-1"
				obc.Namespace = operatorNamespace
				labelValue := createObcLabelValue(ctx, "consumer-1", "obc-1", "ns-1")
				obc.Labels = map[string]string{labelKey: labelValue}
				return []client.Object{obc}
			}(),
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme, err := newScheme()
			if err != nil {
				t.Fatalf("newScheme() error = %v", err)
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.existing...).Build()

			got, err := checkIfObcExists(ctx, fakeClient, operatorNamespace, tt.storageConsumerName, tt.obcName, tt.obcNamespace)
			if err != nil {
				t.Fatalf("checkIfObcExists() unexpected error = %v", err)
			}
			if tt.expected {
				if got == nil {
					t.Fatalf("checkIfObcExists() = nil, want non-nil")
				} else if got.GetName() != obcHashedName {
					t.Fatalf("checkIfObcExists() = %v, want %v", got.GetName(), obcHashedName)
				}
			} else if got != nil {
				t.Fatalf("checkIfObcExists() = %v, want nil", got)
			}
		})
	}
}
