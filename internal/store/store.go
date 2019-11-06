package store

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/runtime"
)

// Interface for writing to storage.
type Interface interface {
	Write(string, runtime.Object) error
	Delete(string, runtime.Object) error
}

// Client for interacting with Git storage.
type Client struct {
	sync.Mutex

	rootDir    string
	repository *git.Repository
}

// New client for interacting with Git storage.
func New(directory string) (Interface, error) {
	client := &Client{
		rootDir: directory,
	}

	r, err := git.PlainOpen(client.rootDir)
	if err != nil {
		return client, err
	}

	client.repository = r

	return client, nil
}

// Write to the storage.
func (c *Client) Write(group string, obj runtime.Object) error {
	// Acquire a lock to ensure we don't have issues with ordering.
	c.Lock()
	defer c.Unlock()

	unstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return errors.Wrap(err, "failed to convert to unstructured object")
	}

	paths, err := GetPaths(c.rootDir, group, unstructured)
	if err != nil {
		return errors.Wrap(err, "failed to get paths")
	}

	data, err := yaml.Marshal(&unstructured)
	if err != nil {
		return errors.Wrap(err, "failed to marshal to yaml")
	}

	if _, err := os.Stat(paths.Directory.Absolute); os.IsNotExist(err) {
		err = os.MkdirAll(paths.Directory.Absolute, 0755)
		if err != nil {
			return errors.Wrap(err, "failed to create directory")
		}
	}

	f, err := os.OpenFile(paths.File.Absolute, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return errors.Wrap(err, "failed to open file")
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return errors.Wrap(err, "failed to write to file")
	}

	message := fmt.Sprintf("Object changed: %s", paths.File.Relative)

	return c.Commit(paths.File.Relative, message)
}

func (c *Client) Delete(group string, obj runtime.Object) error {
	// Acquire a lock to ensure we aren't already committing a file.
	c.Lock()
	defer c.Unlock()

	unstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return errors.Wrap(err, "failed to convert to unstructured object")
	}

	paths, err := GetPaths(c.rootDir, group, unstructured)
	if err != nil {
		return errors.Wrap(err, "failed to get paths")
	}

	err = os.Remove(paths.File.Absolute)
	if err != nil {
		return errors.Wrap(err, "failed to delete file")
	}

	message := fmt.Sprintf("Object deleted: %s", paths.File.Relative)

	return c.Commit(paths.File.Relative, message)
}

func (c *Client) Commit(file, message string) error {
	w, err := c.repository.Worktree()
	if err != nil {
		return errors.Wrap(err, "failed to get worktree")
	}

	s, err := w.Status()
	if err != nil {
		return errors.Wrap(err, "failed to get repository status")
	}

	// There are no changes so we don't have to commit.
	if s.IsClean() {
		return nil
	}

	_, err = w.Add(file)
	if err != nil {
		return errors.Wrap(err, "failed to add file")
	}

	commit, err := w.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "John Doe",
			Email: "john@doe.org",
			When:  time.Now(),
		},
	})
	if err != nil {
		return errors.Wrap(err, "failed to commit")
	}

	log.Printf("%s (%s)\n", message, commit.String())

	return nil
}
