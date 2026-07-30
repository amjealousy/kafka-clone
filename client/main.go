package main

import (
	"fmt"
	"os"

	"kafka-clone/client/cmd"

	"github.com/spf13/cobra"
)

// rootCmd — простой CLI-клиент для kafka-clone: позволяет создавать топики
// (topic create), смотреть их метаданные (topic describe), отправлять
// сообщения (produce) и читать их (consume) без запуска полноценного кода.
var rootCmd = &cobra.Command{
	Use:   "kafka-clone-client",
	Short: "Простой CLI-клиент для kafka-clone (topic/produce/consume)",
}

func main() {
	rootCmd.AddCommand(cmd.TopicCmd, cmd.ProduceCmd, cmd.ConsumeCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
