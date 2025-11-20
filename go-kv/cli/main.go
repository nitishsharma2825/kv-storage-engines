package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

const BaseURL string = "http://localhost:8080"

type ValueBody struct {
	Value string `json:"value"`
}

type KVClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewKVClient(baseURL string) *KVClient {
	return &KVClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *KVClient) Get(key string) (string, error) {
	url := fmt.Sprintf("%s/%s", c.BaseURL, key)

	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return "NOT_FOUND", nil
		}
		return "", fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var result ValueBody
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("invalid JSON response: %w", err)
	}

	return result.Value, nil
}

func (c *KVClient) Put(key string, value string) error {
	url := fmt.Sprintf("%s/%s", c.BaseURL, key)
	body := ValueBody{Value: value}

	// encode to json
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned %d status code", resp.StatusCode)
	}

	return nil
}

func main() {
	client := NewKVClient(BaseURL)
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)

		method := strings.ToLower(fields[0])
		switch method {
		case "get":
			res, err := client.Get(fields[1])
			if err != nil {
				fmt.Printf("Failed get operation for key %s with error: %v\n", fields[1], err)
			}

			fmt.Printf("%s\n", res)
		case "put":
			if err := client.Put(fields[1], fields[2]); err != nil {
				fmt.Printf("Failed put operation for key %s\n", fields[1])
			}

			fmt.Println("Put Success")
		default:
			fmt.Printf("Unsupported method %s\n", method)
		}
	}
}
