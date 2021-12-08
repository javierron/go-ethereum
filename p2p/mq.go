package p2p

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("ERROR: %s, %s", err, msg)
	}
}

func getMQConnection() (*amqp.Connection, error) {
	conn, err := amqp.Dial("amqp://guest:guest@my-rabbit:5672/")
	failOnError(err, "Failed to connect to mq")

	return conn, err
}

func sendMQMsg(conn *amqp.Connection, channel string, msg string) error {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		channel,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "unable to declare exchange")

	err = ch.Publish(
		channel,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		},
	)

	failOnError(err, "failed to publish msg")
	log.Printf(" [x] Sent %s\n", msg)

	return nil
}
