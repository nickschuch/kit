package store

import (
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"
)

const (
	// UnstructuredKeyMetadata is used to extract metadata from an unstructured object.
	UnstructuredKeyMetadata = "metadata"
	// UnstructuredKeyNamespace is used to extract the namespace from an unstructured object.
	UnstructuredKeyNamespace = "namespace"
	// UnstructuredKeyName is used to extract the name from an unstructured object.
	UnstructuredKeyName = "name"
)

type PathList struct {
	Directory Path
	File      Path
}

type Path struct {
	Relative string
	Absolute string
}

func GetPaths(rootDir, group string, unstructured map[string]interface{}) (PathList, error) {
	var list PathList

	namespace, name, err := GetNamespaceName(unstructured)
	if err != nil {
		return list, errors.Wrap(err, "failed to extract metadata")
	}

	fileName := fmt.Sprintf("%s.yml", name)

	list.Directory = Path{
		Relative: filepath.Join(namespace, group),
		Absolute: filepath.Join(rootDir, namespace, group),
	}

	list.File = Path{
		Relative: filepath.Join(namespace, group, fileName),
		Absolute: filepath.Join(rootDir, namespace, group, fileName),
	}

	return list, nil
}

// GetNamespaceName will return a namespace and name from an unstructured object.
func GetNamespaceName(obj map[string]interface{}) (string, string, error) {
	var (
		namespace string
		name      string
	)

	if _, ok := obj[UnstructuredKeyMetadata]; !ok {
		return namespace, name, fmt.Errorf("not found: %s", UnstructuredKeyMetadata)
	}

	metadata := obj[UnstructuredKeyMetadata].(map[string]interface{})

	if _, ok := metadata[UnstructuredKeyNamespace]; !ok {
		return namespace, name, fmt.Errorf("not found: %s", UnstructuredKeyNamespace)
	}

	namespace = metadata[UnstructuredKeyNamespace].(string)

	if _, ok := metadata[UnstructuredKeyName]; !ok {
		return namespace, name, fmt.Errorf("not found: %s", UnstructuredKeyName)
	}

	name = metadata[UnstructuredKeyName].(string)

	return namespace, name, nil
}
