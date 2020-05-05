package main

import(
	"math/rand"
	"strconv"
	"time"
	"fmt"
	"encoding/json"
	"os"

	"github.com/infrawatch/apputils/connector"
	"github.com/infrawatch/apputils/logging"
)

const QDRURL = "amqp://127.0.0.1:5672/lokean/logs"
const LOKIURL = "http://127.0.0.1:3100"

type Log struct {
	Timestamp int    `json:"timestamp"`
	Message   string `json:"message"`
	Source    string `json:"source"`
	Level     string `json:"level"`
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `./loggingTest count_of_logs_sent (default is 5 if not specified)`)
}

func sendLogs(sender *connector.AMQPSender, testUniqueNum int, count int) error {
	timestamp := int(time.Now().Unix() * 1000)
	log := Log {
		Level: "TEST",
		Source: fmt.Sprintf("loggingTest%d", testUniqueNum),
		Timestamp: timestamp,
	}
	for i := 0; i < count; i++ {
		log.Message = strconv.Itoa(i)
		l, err := json.Marshal(log)
		if err != nil {
			return err
		}
		sender.Send(string(l))
		<-sender.GetAckChannel()
	}
	return nil
}

func checkLogs(lokiClient *connector.LokiClient, testUniqueNum int, count int, logger *logging.Logger) (bool, error) {
	source := fmt.Sprintf("loggingTest%d", testUniqueNum)
	queryString := fmt.Sprintf("{source=\"%s\",level=\"TEST\"}", source)
	logger.Metadata(map[string]interface{}{
		"queryString": queryString,
	})
	logger.Debug("Querying logs")

	messages, err := lokiClient.Query(queryString)
	if err != nil {
		return false, err
	}
	if len(messages) != count {
		logger.Metadata(map[string]interface{}{
			"returned": messages,
			"amount of messages in loki": len(messages),
			"expected amount of messages in loki": count,
		})
		logger.Info("Different amount of messages in loki, then expected")
		return false, nil
	}
	for i := 0; i < count; i++ {
		found := false
		for _, message := range messages {
			logNum, err := strconv.Atoi(message.Message)
			if err != nil {
				logger.Metadata(map[string]interface{}{
					"expected": "number",
					"got": message.Message,
				})
				logger.Info("A wrong message format returned from loki")
			}
			if i == logNum {
				found = true
				break
			}
		}
		if !found {
			logger.Metadata(map[string]interface{}{
				"Searched message": i,
				"All messages": messages,
			})
			logger.Info("Didn't find a message in loki")
			return false, nil
		}
	}
	return true, nil
}

func main() {
	var count int
	if len(os.Args) == 1 {
		count = 5
	} else if len(os.Args) == 2 {
		var err error
		count, err = strconv.Atoi(os.Args[1])
		if err != nil {
			printUsage()
			os.Exit(1)
		}
	} else {
		printUsage()
		os.Exit(1)
	}
	generator := rand.New(rand.NewSource(time.Now().UnixNano()))
	testUniqueNum := generator.Int()

	logger, err := logging.NewLogger(logging.DEBUG, "/dev/stderr")
	if err != nil {
		fmt.Println("Failed to initialize logger")
		os.Exit(2)
	}
	defer logger.Destroy()

	sender := connector.NewAMQPSender(QDRURL, true, logger)
	lokiClient, err := connector.NewLokiConnector(LOKIURL, 2, 100)
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Error while initializing lokiClient")
	}

	err = sendLogs(sender, testUniqueNum, count)
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Error while sending logs")
	}

	time.Sleep(500 * time.Millisecond)

	passed, err := checkLogs(lokiClient, testUniqueNum, count, logger)
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Error while checking logs")
	}
	if passed {
		logger.Warn("SUCCESS!")
	} else {
		logger.Warn("TEST Failed, the logs sent through amqp didn't appear inside loki")
	}
}
