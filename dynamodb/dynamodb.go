// package dynamodb implements ServiceDiscovery interface using dynamodb tables.
package dynamodb

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	sd "github.com/msf/servicediscovery"
)

const DefaultAwsRegion = "eu-west-1"

type ServiceWorkerDdb struct {
	ServiceId string    `dynamodbav:"serviceId"`
	WorkerId  string    `dynamodbav:"workerId"`
	LastSeen  time.Time `dynamodbav:"last_seen_time,unixtime"`
	Endpoint  string    `dynamodbav:"endpoint"`

	Role        string `dynamodbav:"role"`
	DeployGroup string `dynamodbav:"deploy_group"`
	ServiceName string `dynamodbav:"service_name"`
}

// ServiceDiscoveryDynamoDB holds the details  required to implement a ServiceDiscovery using dynamodb
// TODO: should this be private? name is too long
type ServiceDiscoveryDynamoDB struct {
	ddb       *dynamodb.DynamoDB
	tableName string
}

// NewDynamoDBLocal uses a local instance of dynamodb running on endpoint
func NewDynamoDBLocal(cfg LocalConfig) (*ServiceDiscoveryDynamoDB, error) {
	creds := credentials.NewStaticCredentials("123", "123", "")
	awsConfig := &aws.Config{
		Credentials: creds,
		Region:      &cfg.AwsRegion,
		Endpoint:    &cfg.Endpoint,
	}
	db := dynamodb.New(session.New(awsConfig))
	err := createTableIfNotExists(db, cfg.DdbTableName)
	if err != nil {
		return nil, err
	}

	return &ServiceDiscoveryDynamoDB{
		ddb:       db,
		tableName: cfg.DdbTableName,
	}, nil
}

// New will take in the Config provided and attempt to create a connection to
// DynamoDB. If the cfg does not set the Access and Secret key (if they are the)
// empty string, it will fall back to the ENV variables and the IAM credentials
// of the instance (if running in EC2)
func New(cfg Config) (*ServiceDiscoveryDynamoDB, error) {
	if err := cfg.HasError(); err != nil {
		return nil, err
	}

	awsCfg := aws.NewConfig()
	if len(cfg.AwsRegion) > 0 {
		awsCfg = awsCfg.WithRegion(cfg.AwsRegion)
	}
	if len(cfg.AwsAccessKey) > 0 && len(cfg.AwsSecretKey) > 0 {
		awsCfg = awsCfg.WithCredentials(credentials.NewStaticCredentials(cfg.AwsAccessKey, cfg.AwsSecretKey, ""))
	}

	db := dynamodb.New(session.New(awsCfg))

	return &ServiceDiscoveryDynamoDB{
		ddb:       db,
		tableName: cfg.DdbTableName,
	}, nil
}

func (svc *ServiceDiscoveryDynamoDB) ListWorkers(service sd.ServiceRole, earliestLastSeen time.Time) ([]*sd.ServiceWorker, error) {
	var workers []*sd.ServiceWorker
	serviceIdKeyCondition := "serviceId=:id"
	queryParams := &dynamodb.QueryInput{
		TableName:              aws.String(svc.tableName),
		KeyConditionExpression: &serviceIdKeyCondition,
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":id": &dynamodb.AttributeValue{S: aws.String(service.ServiceId())},
		},
	}

	err := svc.ddb.QueryPages(
		queryParams,
		func(p *dynamodb.QueryOutput, lastPage bool) bool {
			for _, item := range p.Items {
				worker := createWorkerFromItem(item, &service)
				if !worker.LastSeen.Before(earliestLastSeen) {
					workers = append(workers, worker)
				}
			}
			return true
		})
	if err != nil {
		return workers, fmt.Errorf(
			"error on ddb.QueryPages(%v, serviceId==%v) err: %v",
			svc.tableName, service.ServiceId(), err)
	}

	return workers, nil
}

func (svc *ServiceDiscoveryDynamoDB) RegisterWorker(worker sd.ServiceWorker, heartBeat time.Time) error {
	ddbItem, err := convertToDynamoDBItem(worker, heartBeat)
	if err != nil {
		return fmt.Errorf("failure to marshal onto ddb attribute: %+v, err: %v", worker, err)
	}

	_, err = svc.ddb.PutItem(&dynamodb.PutItemInput{
		Item:      ddbItem,
		TableName: aws.String(svc.tableName),
	})
	if err != nil {
		return fmt.Errorf("failure on ddb.PutItem(%+v), err: %v", ddbItem, err)
	}
	return nil
}

func createWorkerFromItem(attributes map[string]*dynamodb.AttributeValue, service *sd.ServiceRole) *sd.ServiceWorker {
	worker := &sd.ServiceWorker{
		Service: *service,
	}
	for name, attrib := range attributes {
		switch name {
		case "last_seen_time":
			i, _ := strconv.ParseInt(*attrib.N, 10, 64)
			worker.LastSeen = time.Unix(i, 0)
		case "endpoint":
			worker.Endpoint = *attrib.S
		case "workerId":
			worker.WorkerId = *attrib.S
		}
	}
	return worker
}

func convertToDynamoDBItem(worker sd.ServiceWorker, now time.Time) (map[string]*dynamodb.AttributeValue, error) {
	item := &ServiceWorkerDdb{
		LastSeen:    now.UTC(),
		ServiceId:   worker.Service.ServiceId(),
		WorkerId:    worker.WorkerId,
		Endpoint:    worker.Endpoint,
		Role:        worker.Service.Role,
		DeployGroup: worker.Service.DeployGroup,
		ServiceName: worker.Service.ServiceName,
	}
	return dynamodbattribute.MarshalMap(item)
}

func createTableIfNotExists(ddb *dynamodb.DynamoDB, tableName string) error {
	params := &dynamodb.DescribeTableInput{TableName: &tableName}
	out, err := ddb.DescribeTable(params)
	if err != nil {
		return createTableTiny(ddb, tableName)
	}
	if err == nil && *out.Table.TableStatus != "ACTIVE" {
		return fmt.Errorf("DDB Table exists but isn't active, status: %v", out.Table.TableStatus)
	}
	return nil
}

// createTableTiny creates a table with the correct schema but very little IOPS.
func createTableTiny(ddb *dynamodb.DynamoDB, tableName string) error {
	params := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("serviceId"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("workerId"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("serviceId"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("workerId"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(5),
		},
	}

	_, err := ddb.CreateTable(params)
	if err != nil {
		return fmt.Errorf("DDB createTable failed: %v", err)
	}
	// wait for table to become available
	var out *dynamodb.DescribeTableOutput
	for i := 0; i < 5; i++ {
		out, err = ddb.DescribeTable(&dynamodb.DescribeTableInput{TableName: &tableName})
		if err != nil {
			return fmt.Errorf("describeTable failed: %v", err)
		}
		switch *out.Table.TableStatus {
		case "CREATING":
			time.Sleep(100 * time.Millisecond)
		case "ACTIVE":
			return nil
		}
	}
	return fmt.Errorf("Waited too long for table to be ready, status: %v", out.Table.TableStatus)
}
