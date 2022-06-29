package dynamodb

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	sd "github.com/msf/servicediscovery"
)

// setupTestLocalDDB is for running unit tests agains a local dynamodb instance running at: http://localhost:8844
func setupTestLocalDDB(t *testing.T) sd.ServiceDiscovery {
	unitestConfig := LocalConfig{
		DdbTableName: "unittest-table",
		Endpoint:     "http://localhost:8844",
		AwsRegion:    "eu-west-1",
	}
	svc, err := NewDynamoDBLocal(unitestConfig)
	if err != nil {
		t.Fatalf("Failed to create ServiceDiscoveryDynamoDB: err: %v", err)
	}
	return svc
}

func TestItemToDdbItem(t *testing.T) {
	worker := sd.ServiceWorker{
		Service: sd.ServiceRole{
			Role:        "fetcher",
			DeployGroup: "testRegion",
			ServiceName: "foo-service",
		},
		WorkerId: "workerId",
		Endpoint: "/dev/null",
	}
	now := time.Now()
	expectedMap := map[string]*dynamodb.AttributeValue{
		"workerId":       {S: aws.String(worker.WorkerId)},
		"serviceId":      {S: aws.String(worker.Service.ServiceId())},
		"last_seen_time": {N: aws.String(strconv.FormatInt(now.Unix(), 10))},
		"endpoint":       {S: aws.String(worker.Endpoint)},
		"role":           {S: aws.String(worker.Service.Role)},
		"deploy_group":   {S: aws.String(worker.Service.DeployGroup)},
		"service_name":   {S: aws.String(worker.Service.ServiceName)},
	}

	result, err := convertToDynamoDBItem(worker, now)
	if err != nil {
		t.Fatalf("failed to convert: %v, err: %v", worker, err)
	}
	if !reflect.DeepEqual(expectedMap, result) {
		t.Errorf("expected: %+v, got: %+v", expectedMap, result)
	}

}

func TestDynamoDBServiceDiscovery(t *testing.T) {
	worker := sd.ServiceWorker{
		Service: sd.ServiceRole{
			Role:        "fetcher",
			DeployGroup: "testRegion",
			ServiceName: "foo-service",
		},
		WorkerId: "workerId",
		Endpoint: "/dev/null",
	}
	svc := setupTestLocalDDB(t)
	now := time.Now()
	err := svc.RegisterWorker(worker, now)
	if err != nil {
		t.Errorf("Failed to register worker: %v, err: %v", worker, err)
	}

	worker.LastSeen = time.Unix(now.Unix(), 0)
	validateListWorkersLength(t, svc, worker.Service, time.Time{}, []sd.ServiceWorker{worker})
}

func validateListWorkersLength(t *testing.T, svc sd.ServiceDiscovery, service sd.ServiceRole, now time.Time, expectedWorkers []sd.ServiceWorker) {

	workers, err := svc.ListWorkers(service, now)
	if err != nil {
		t.Errorf("error on ListWorkers: %v, service: %v", err, service)
	}
	if count := len(workers); count != len(expectedWorkers) {
		t.Errorf("Expected %d worker, found: %d", len(expectedWorkers), count)
	}

	for i, worker := range workers {
		if !reflect.DeepEqual(*worker, expectedWorkers[i]) {
			t.Errorf("Worker at index: %d, expected: \n%v, got: \n%v", i, expectedWorkers[i], *worker)
		}
	}
}
