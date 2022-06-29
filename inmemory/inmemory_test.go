package inmemory

import (
	"reflect"
	"testing"
	"time"

	sd "github.com/msf/servicediscovery"
)

func TestServiceDiscoveryBasic(t *testing.T) {
	svc := New()

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
	err := svc.RegisterWorker(worker, now)
	if err != nil {
		t.Errorf("Failed to register worker: %v, err: %v", worker, err)
	}

	worker.LastSeen = now
	validateListWorkersLength(t, svc, worker.Service, time.Time{}, []sd.ServiceWorker{worker})
}

func TestServiceDiscoveryReregister(t *testing.T) {
	svc := New()

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
	err := svc.RegisterWorker(worker, now)
	if err != nil {
		t.Errorf("Failed to register worker: %v, err: %v", worker, err)
	}
	worker.LastSeen = now
	validateListWorkersLength(t, svc, worker.Service, time.Time{}, []sd.ServiceWorker{worker})

	worker.Endpoint = "new-endpoint"
	now = time.Now()
	err = svc.RegisterWorker(worker, now)
	worker.LastSeen = now
	validateListWorkersLength(t, svc, worker.Service, time.Time{}, []sd.ServiceWorker{worker})
}

func TestFailure(t *testing.T) {
	svc := New()

	worker := sd.ServiceWorker{
		Service: sd.ServiceRole{
			Role:        "fetcher",
			DeployGroup: "testRegion",
			ServiceName: "foo-service",
		},
		WorkerId: "workerId",
		Endpoint: "/dev/null",
	}

	err := svc.RegisterWorker(worker, time.Unix(FailRegistrationAtTimestamp, 0))
	if err == nil {
		t.Errorf("expected error for failure timestamp, got nil")
	}
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
