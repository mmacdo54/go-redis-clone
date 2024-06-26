package configuration

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

type Config struct {
	Requirepass bool
	password    string
}

func InitConfig() (Config, error) {
	config := Config{}
	file, err := os.OpenFile("./redis.conf", os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return config, err
	}

	reader := bufio.NewReader(file)

	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return config, err
		}
		if strings.HasPrefix(string(line), "#") {
			continue
		}
		key, value, _ := strings.Cut(string(line), " ")
		switch key {
		case "requirepass":
			if err := config.parsePasswordConfig(value); err != nil {
				return config, err
			}
		default:
			continue
		}
	}

	return config, nil
}

func (config *Config) parsePasswordConfig(password string) error {
	config.Requirepass = true
	match, err := regexp.MatchString(`^[a-zA-Z0-9!&#$^<>-]{16,128}$`, password)
	if err != nil {
		return err
	}
	if !match {
		return fmt.Errorf("Invalid password supplied")
	}
	config.password = password
	return nil
}

func (config *Config) ValidatePassword(input string) error {
	if input != config.password {
		return fmt.Errorf("Wrong password")
	}

	return nil
}
