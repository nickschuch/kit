package main

import (
	"log"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/nickschuch/kit/internal/store"
)

var (
	cliMasterURL  = kingpin.Flag("master", "URL of the Kubernetes master").String()
	cliKubeConfig = kingpin.Flag("kubeconfig", "Path to the ~/.kube/config file").Envar("KUBECONFIG").String()
	cliRepository = kingpin.Arg("repository", "Path to the Git repository").Envar("KIT_REPOSITORY").String()
)

func main() {
	kingpin.Parse()

	config, err := clientcmd.BuildConfigFromFlags(*cliMasterURL, *cliKubeConfig)
	if err != nil {
		panic(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	client, err := store.New(*cliRepository)
	if err != nil {
		panic(err)
	}

	err = watch(clientset, client)
	if err != nil {
		panic(err)
	}
}

// Watch for API object changes.
func watch(clientset kubernetes.Interface, client store.Interface) error {
	factory := informers.NewSharedInformerFactory(clientset, 0)

	pods := factory.Core().V1().Pods().Informer()
	pods.AddEventHandler(EventHander(client, "pod"))

	svc := factory.Core().V1().Services().Informer()
	svc.AddEventHandler(EventHander(client, "service"))

	endpoints := factory.Core().V1().Endpoints().Informer()
	endpoints.AddEventHandler(EventHander(client, "endpoint"))

	stop := make(chan struct{})
	defer close(stop)
	factory.Start(stop)
	for {
		time.Sleep(time.Second)
	}
}

func EventHander(c store.Interface, group string) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			err := c.Write(group, obj.(runtime.Object))
			if err != nil {
				log.Println("failed to add:", err)
			}
		},
		DeleteFunc: func(obj interface{}) {
			err := c.Delete(group, obj.(runtime.Object))
			if err != nil {
				log.Println("failed to delete:", err)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			err := c.Write(group, newObj.(runtime.Object))
			if err != nil {
				log.Println("failed to update:", err)
			}
		},
	}
}
