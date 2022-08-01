package config

import (
	"context"
	"fmt"
	"github.com/coinbase/redisbetween/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"io/fs"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func createConfig(t *testing.T, data string) *os.File {
	f, err := ioutil.TempFile("", "*-config.json")
	assert.NoError(t, err)
	assert.NoError(t, f.Close())

	bytes := []byte(data)
	assert.NoError(t, ioutil.WriteFile(f.Name(), bytes, fs.ModePerm))
	return f
}

func TestLoadConfigFromAFile(t *testing.T) {
	url := uuid.New().String()
	f := createConfig(t, fmt.Sprintf("{\"url\": \"%s\" }", url))

	defer func() {
		assert.NoError(t, os.Remove(f.Name()))
	}()

	opts := Options{Url: f.Name(), PollInterval: 1000}
	config, err := Load(context.WithValue(context.TODO(), utils.CtxLogKey, zap.L()), &opts)
	assert.NoError(t, err)
	defer config.Stop()

	cfg, _ := config.Config()
	assert.Equal(t, url, cfg.Url)
}

func TestPollConfigFromAFile(t *testing.T) {
	url := uuid.New().String()
	f := createConfig(t, fmt.Sprintf("{\"url\": \"%s\" }", url))

	defer func() {
		assert.NoError(t, os.Remove(f.Name()))
	}()

	opts := Options{Url: f.Name(), PollInterval: 1}
	config, err := Load(context.WithValue(context.TODO(), utils.CtxLogKey, zap.L()), &opts)
	assert.NoError(t, err)
	defer config.Stop()

	cfg, _ := config.Config()
	assert.Equal(t, url, cfg.Url)

	newUrl := uuid.New().String()
	jsonConfig := []byte(fmt.Sprintf("{\"url\": \"%s\" }", newUrl))
	assert.NoError(t, ioutil.WriteFile(f.Name(), jsonConfig, fs.ModePerm))

	time.Sleep(2 * time.Second)
	cfg, _ = config.Config()
	assert.NotEqual(t, url, cfg.Url)
	assert.Equal(t, newUrl, cfg.Url)
}
