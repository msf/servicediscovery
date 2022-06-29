// inmemory implements a in memory only service discovery
package inmemory

import (
	"fmt"
	"sync"
	"time"

	sd "github.com/msf/servicediscovery"
)

// ServiceDiscoveryInMemory holds the inmemory data required to implement a ServiceDiscovery
// TODO: should this be private? name is too long
type ServiceDiscoveryInMemory struct {
	mu      sync.RWMutex
	workers map[string]workerGroup
}

// workerGroup maps workerId to worker
type workerGroup map[string]*sd.ServiceWorker

const (
	// FailRegistrationAtTimestamp is used to return an error if the registration heartbeat time
	// is this particular timestamp (used for testing)
	FailRegistrationAtTimestamp = 20000

	// FailListWorkersAtTimestamp is used return an error if the list workers earliest last seen time
	// is this particular timestamp (used for testing)
	FailListWorkersAtTimestamp = 20000
)

func New() *ServiceDiscoveryInMemory {
	return &ServiceDiscoveryInMemory{
		workers: make(map[string]workerGroup),
	}
}

func (inmem *ServiceDiscoveryInMemory) RegisterWorker(worker sd.ServiceWorker, heartbeat time.Time) error {
	inmem.mu.Lock()
	defer inmem.mu.Unlock()

	if heartbeat.Unix() == FailRegistrationAtTimestamp {
		return fmt.Errorf("Failed because your timestamp is the FailInMemoryAtTimestamp")
	}

	worker.LastSeen = heartbeat
	serviceId := worker.Service.ServiceId()
	currWorkers, ok := inmem.workers[serviceId]
	if !ok {
		currWorkers = make(workerGroup)
		inmem.workers[serviceId] = currWorkers
	}
	currWorkers[worker.WorkerId] = &worker
	return nil
}

func (inmem *ServiceDiscoveryInMemory) ListWorkers(service sd.ServiceRole, earliestLastSeen time.Time) ([]*sd.ServiceWorker, error) {
	inmem.mu.RLock()
	defer inmem.mu.RUnlock()

	if earliestLastSeen.Unix() == FailListWorkersAtTimestamp {
		return nil, fmt.Errorf("Failed because your timestamp is the FailInMemoryAtTimestamp")
	}

	var workers []*sd.ServiceWorker

	currWorkers, ok := inmem.workers[service.ServiceId()]
	if !ok {
		return workers, nil
	}
	for _, worker := range currWorkers {
		if !worker.LastSeen.Before(earliestLastSeen) {
			workers = append(workers, worker)
		}
	}
	return workers, nil
}
