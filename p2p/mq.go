package p2p

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type SerializableMsg struct {
	Code       uint64
	Size       uint32 // Size of the raw payload
	Payload    []byte
	ReceivedAt time.Time

	MeterCap  Cap    // Protocol name and version for egress metering
	MeterCode uint64 // Message within protocol for egress metering
	MeterSize uint32 // Compressed message size for ingress metering
}

func (msg SerializableMsg) String() string {
	return fmt.Sprintf("msg #%v (%v bytes)", msg.Code, msg.Size)
}

func MsgSerialize(msg Msg) (ser *SerializableMsg, err error) {

	buf := new(bytes.Buffer)
	buf.ReadFrom(msg.Payload)

	s := &SerializableMsg{
		msg.Code,
		msg.Size,
		buf.Bytes(),
		msg.ReceivedAt,
		msg.meterCap,
		msg.meterCode,
		msg.meterSize,
	}

	return s, nil
}

func MsgDeserialize(ser *SerializableMsg) (*Msg, error) {

	des := &Msg{
		ser.Code,
		ser.Size,
		bytes.NewReader(ser.Payload),
		ser.ReceivedAt,
		ser.MeterCap,
		ser.MeterCode,
		ser.MeterSize,
	}

	return des, nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatal("%s:%s", msg, err)
	}
}

func SetupMQConnection() *amqp.Connection {
	conn, err := amqp.Dial("amqp://guest:guest@my-rabbit/")
	failOnError(err, "Failed to connect")

	return conn
}

func SetupMQChannel(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	failOnError(err, "failed to get channel")

	return ch
}

func bind_mq(exchange_name string, ch *amqp.Channel) *amqp.Queue {
	exchange := exchange_name + "-in"
	err := ch.ExchangeDeclare(
		exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare queue")

	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)

	failOnError(err, "error on queue declar")

	err = ch.QueueBind(
		q.Name,
		"",
		exchange,
		false,
		nil,
	)

	failOnError(err, "fail on queue bind")

	return &q

}

func read_from_mq(exchange_name string, ch *amqp.Channel, q *amqp.Queue, read_channel chan Msg) {
	exchange := exchange_name + "-in"

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	failOnError(err, "failed to register consumer")

	log.Printf("[SUBNODE: %s] waiting for messages", exchange)

	for d := range msgs {

		m := SerializableMsg{}
		// dec := gob.NewDecoder(bytes.NewReader(d.Body))
		// dec.Decode(&m)

		err = json.Unmarshal(d.Body, &m)
		failOnError(err, "failed to unmarshall")

		msg, _ := MsgDeserialize(&m)

		read_channel <- *msg
		log.Printf("[SUBNODE: %s] received message: %d", exchange, msg.Code)
	}

}

func read_from_mq_for_peers(ch amqp.Channel, read_channel chan string) {

	err := ch.ExchangeDeclare(
		"add_peer",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare queue")

	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)

	failOnError(err, "error on queue declar")

	err = ch.QueueBind(
		q.Name,
		"",
		"add_peer",
		false,
		nil,
	)

	failOnError(err, "fail on queue bind")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	failOnError(err, "failed to register consumer")

	log.Printf(" [SUBNODE add_peer] waiting for messages")

	for d := range msgs {

		log.Printf("[SUBNODE add_peer] received message: %s", d.Body)
		read_channel <- string(d.Body)
	}

}

func write_to_mq(exchange_name string, ch *amqp.Channel, write_channel chan Msg) {
	exchange := exchange_name + "-out"
	err := ch.ExchangeDeclare(
		exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "failed to declare queue")

	// var b bytes.Buffer
	// e := gob.NewEncoder(&b)

	for msg := range write_channel {

		ser, err := MsgSerialize(msg)

		if err != nil {
			failOnError(err, "Unable to transform to serializable")
		}

		// b.Reset()
		// err = e.Encode(*ser)

		b, err := json.Marshal(ser)

		if err != nil {
			failOnError(err, "Unable to encode message")
		}

		err = ch.Publish(
			exchange,
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        b,
			},
		)

		failOnError(err, "failed to publish msg")
		log.Printf(" [SUBNODE %s] Sent %s\n", exchange, ser.String())
	}

}
