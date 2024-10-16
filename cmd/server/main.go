package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Create a channel
	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer publishCh.Close()

	fmt.Println("Successfully connected to RabbitMQ")
	gamelogic.PrintServerHelp()

	ch, q, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		"game_logs.*",
		pubsub.SimpleQueueDurable,
	)
	if err != nil {
		log.Fatalf("Failed to declare and bind queue: %v", err)
	}
	defer ch.Close()
	fmt.Printf("Queue '%s' created and bound to exchange '%s' with routing key '%s'\n", q.Name, routing.ExchangePerilTopic, "game_logs.*")

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueDurable,
		handlerLogs(),
	)
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		command := words[0]

		switch command {
		case "pause":
			fmt.Println("Sending pause message...")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("Failed to publish pause message: %v", err)
			} else {
				fmt.Println("Successfully published pause message to exchange")
			}

		case "resume":
			fmt.Println("Sending resume message...")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("Failed to publish resume message: %v", err)
			} else {
				fmt.Println("Successfully published resume message to exchange")
			}

		case "quit":
			fmt.Println("Exiting...")
			return

		default:
			fmt.Println("I don't understand that command")
		}
	}
}
