package main

import (
	"os"

	"github.com/bobguo/mysql-replay/cmd"
	"go.uber.org/zap"
)

func main() {
	if err := cmd.NewRootCmd().Execute(); err != nil {
		zap.L().Error("error exit: "+err.Error(), zap.Error(err))
		os.Exit(1)
	}
}
