# fargocd

```
kubectl create -f https://github.com/fluxcd/helm-controller/raw/v1.2.0/config/crd/bases/helm.toolkit.fluxcd.io_helmreleases.yaml

kubectl create -f https://github.com/fluxcd/source-controller/raw/v1.5.0/config/crd/bases/source.toolkit.fluxcd.io_helmrepositories.yaml
```

```
kubectl create ns flux-system

kubectl create -f https://github.com/appscodelabs/tasty-kube/raw/master/fluxcd/helmrelease.yaml
```
