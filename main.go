// kafka-cli project main.go
package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func hash_group(t time.Time) string {

	const Size = 7

	hash := sha1.New()
	hash.Write([]byte(t.String()))
	group_hash := hex.EncodeToString(hash.Sum(nil))

	return group_hash[:7]
}

var (
	offset   = kingpin.Flag("offset", "Offset where to start from (auto.offset.reset).").Default("earliest").Short('o').String()
	broker   = kingpin.Flag("broker", "Which broker to poll the messages from.").Default("localhost:29092").Short('b').String()
	group    = kingpin.Flag("group", "Consumer Group to register. Will be defaulted randomly.").Short('g').Default(hash_group(time.Now())).String()
	topic    = kingpin.Flag("topic", "Which topic should be used.").Short('T').Required().String()
	tseconds = kingpin.Flag("timeout", "Session Timeout in seconds.").Default("6").Short('t').String()
	verbose  = kingpin.Flag("verbose", "Verbose mode.").Short('v').Bool()
)

func main() {

	kingpin.Parse()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	timeout, _ := strconv.Atoi(*tseconds)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     *broker,
		"group.id":              *group,
		"auto.offset.reset":     *offset,
		"session.timeout.ms":    timeout * 1000,
		"broker.address.family": "v4",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr,
			"Failed to create a consumer to broker: %s with group: s% and topic: %s.\n",
			*broker, *group, *topic)

		if *verbose {
			fmt.Fprintln(os.Stderr, "%s", err)
		}

		os.Exit(1)
	}

	fmt.Printf("Successful created consumer: %v.\n", c)

	defer func() {
		fmt.Println("Closing consumer.")
		c.Close()
	}()

	err = c.SubscribeTopics([]string{*topic, "^aRegex.*[Tt]opic"}, nil)

	if err != nil {
		fmt.Fprintln(os.Stderr,
			"Failed to subscribe topic %s.", *topic)

		if *verbose {
			fmt.Fprintln(os.Stderr, "%s", err)
		}

		os.Exit(1)
	}

	run := true

	if *verbose {
		fmt.Fprintf(os.Stdout, "Subscribed topic: %s with group: %s.\n", *topic, *group)
	}

	for run == true {
		select {

		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating.\n", sig)
			run = false

		default:
			event := c.Poll(100)
			if event == nil {
				continue
			}

			switch e := event.(type) {

			case *kafka.Message:
				fmt.Printf("Recieved message on %s:\n%s.\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil && *verbose == true {
					fmt.Printf("With headers: %v.\n", e.Headers)
				}
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v: %v.\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
}
