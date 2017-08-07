package utils

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// Read http Get请求中取数据
func Get(requestURL string, params url.Values, body interface{}) error {
	if strings.Contains(requestURL, "?") && params != nil {
		requestURL += params.Encode()
	} else {
		requestURL += "?" + params.Encode()
	}
	req, _ := http.NewRequest("GET", requestURL, nil)
	resp, _ := http.DefaultClient.Do(req)
	return parseResponse(resp, body)
}

// Post http post请求中取数据
func Post(requestURL string, params string, body interface{}) error {
	reader := strings.NewReader(params)
	req, _ := http.NewRequest("POST", requestURL, reader)
	resp, _ := http.DefaultClient.Do(req)
	return parseResponse(resp, body)
}

func parseResponse(resp *http.Response, body interface{}) (err error) {
	defer resp.Body.Close()
	if body == nil {
		return
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	return json.Unmarshal(data, body)
}
