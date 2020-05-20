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
	"github.com/infrawatch/apputils/config"
)

type Log struct {
	Timestamp int    `json:"timestamp"`
	Message   string `json:"message"`
	Source    string `json:"source"`
	Level     string `json:"level"`
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `./loggingTest config_path count_of_logs_sent (default is 5 if not specified)`)
}

func sendLogs(sender chan interface{}, testUniqueNum int, count int) error {
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
		sender <- connector.AMQP10Message{Address: "lokean/logs", Body: string(l)}
	}
	return nil
}

func checkLogs(lokiConnector *connector.LokiConnector, testUniqueNum int, count int, logger *logging.Logger) (bool, error) {
	source := fmt.Sprintf("loggingTest%d", testUniqueNum)
	queryString := fmt.Sprintf("{source=\"%s\"}", source)
	logger.Metadata(map[string]interface{}{
		"queryString": queryString,
	})
	logger.Debug("Querying logs")

	messages, err := lokiConnector.Query(queryString, 0, count)
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
			var logNum int
			_, err := fmt.Sscanf(message.Message, "[TEST] %d", &logNum)
			if err != nil {
				logger.Metadata(map[string]interface{}{
					"expected": "[TEST] number",
					"got": message.Message,
					"error": err,
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
	count := 5
	if len(os.Args) != 2 && len(os.Args) != 3 {
		printUsage()
		os.Exit(1)
	}
	configPath := os.Args[1]
	if len(os.Args) == 3 {
		var err error
		count, err = strconv.Atoi(os.Args[2])
		if err != nil {
			printUsage()
			os.Exit(1)
		}
	}
	generator := rand.New(rand.NewSource(time.Now().UnixNano()))
	testUniqueNum := generator.Int()

	logger, err := logging.NewLogger(logging.DEBUG, "/dev/stderr")
	if err != nil {
		fmt.Println("Failed to initialize logger")
		os.Exit(2)
	}
	defer logger.Destroy()

	metadata := map[string][]config.Parameter{
		"amqp1": []config.Parameter{
			config.Parameter{Name: "connection", Tag: "", Default: "amqp://localhost:5672/lokean/logs", Validators: []config.Validator{}},
			config.Parameter{Name: "send_timeout", Tag: "", Default: 2, Validators: []config.Validator{config.IntValidatorFactory()}},
			config.Parameter{Name: "client_name", Tag: "", Default: "test", Validators: []config.Validator{}},
		},
		"loki": []config.Parameter{
			config.Parameter{Name: "connection", Tag: "", Default: "http://localhost:3100", Validators: []config.Validator{}},
			config.Parameter{Name: "batch_size", Tag: "", Default: 20, Validators: []config.Validator{config.IntValidatorFactory()}},
			config.Parameter{Name: "max_wait_time", Tag: "", Default: 100, Validators: []config.Validator{config.IntValidatorFactory()}},
		},
	}
	conf := config.NewINIConfig(metadata, logger)
	err = conf.Parse(configPath)
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Error while parsing config file")
		return
	}

	amqp, err := connector.NewAMQP10Connector(conf, logger)
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Couldn't connect to AMQP")
		return
	}
	err = amqp.Connect()
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Error while connecting to AMQP")
		return
	}
	amqpReceiver := make(chan interface{})
	amqpSender := make(chan interface{})
	amqp.Start(amqpReceiver, amqpSender)

	err = sendLogs(amqpSender, testUniqueNum, count)
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Error while sending logs")
		return
	}

	// Wait a bit, so that the logs get properly saved in Loki
	time.Sleep(500 * time.Millisecond)

	loki, err := connector.NewLokiConnector(conf, logger)
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Couldn't connect to Loki")
		return
	}
	err = loki.Connect()
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Couldn't connect to Loki")
		return
	}
	lokiReceiver := make(chan interface{})
	lokiSender := make(chan interface{})
	loki.Start(lokiReceiver, lokiSender)

	passed, err := checkLogs(loki, testUniqueNum, count, logger)
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Error while checking logs")
		return
	}
	if passed {
		logger.Warn("SUCCESS!")
	} else {
		logger.Warn("TEST Failed, the logs sent through amqp didn't appear inside loki")
	}
}
