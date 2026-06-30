package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

type Config struct {
	Port    int
	Verbose bool
	Env     string
}

var cfg Config

var rootCmd = &cobra.Command{
	Use:   "app",
	Short: "App — пример стартовой команды",
	Long:  `Это пример запуска приложения с одновременным использованием флагов и тегов.`,

	// ТЕГИ КОМАНДЫ (Annotations) — полезно для категоризации или логики внутри плагинов
	Annotations: map[string]string{
		"tier":        "production",
		"module":      "server",
		"auth-exempt": "true",
	},

	// Основная логика, выполняемая при запуске команды
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("=== Запуск стартовой команды kafka client ===")

		// 1. Флаги уже автоматически записаны в структуру cfg благодаря указателям в init()
		fmt.Printf("Флаги приложения:\n")
		fmt.Printf("  - Порт: %d\n", cfg.Port)
		fmt.Printf("  - Окружение: %s\n", cfg.Env)
		fmt.Printf("  - Подробный вывод (Verbose): %v\n", cfg.Verbose)

		// 2. Чтение встроенных тегов (аннотаций) команды
		fmt.Printf("\nТеги (Аннотации) команды:\n")
		fmt.Printf("  - Уровень (tier): %s\n", cmd.Annotations["tier"])
		fmt.Printf("  - Модуль: %s\n", cmd.Annotations["module"])

		if cmd.Annotations["auth-exempt"] == "true" {
			fmt.Println("\n[Инфо] Проверка авторизации для этой команды пропущена.")
		}
	},
}

func main() {
	rootCmd.Flags().IntVarP(&cfg.Port, "port", "p", 8080, "Порт для веб-сервера")
	rootCmd.Flags().StringVarP(&cfg.Env, "env", "e", "development", "Среда выполнения (dev, prod)")
	rootCmd.Flags().BoolVarP(&cfg.Verbose, "verbose", "v", false, "Включить детализированный лог")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
