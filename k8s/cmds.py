from collections import namedtuple

Shell = namedtuple("Shell", ["cmd", "desc"])
Template = namedtuple("Template", ["path", "desc"])

cmds = {
    "echo": [
        Shell("echo hello 1", "echoing first"),
        Shell("echo hello 2", "echoing second"),
        Shell("bad_command_garbage", "Something that will fail"),
        Shell("echo hello 3", "echoing third which we wont see"),
    ],
    "k3s_setup": [
        Shell("curl -sfL https://get.k3s.io | sh -", "Installing K3s"),
        Shell(
            "chmod a+r /etc/rancher/k3s/k3s.yaml",
            "Allow read access to chmod a+r /etc/rancher/k3s/k3s.yaml",
        ),
        Shell(
            "curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash",
            "Installing Helm",
        ),
        Shell(
            "helm repo add kuberay https://ray-project.github.io/kuberay-helm/",
            "Adding kube ray helm repo",
        ),
        Shell(
            "helm repo add spark-operator https://kubeflow.github.io/spark-operator",
            "Adding spark operator helm repo",
        ),
        Shell("helm repo update", "Updating helm repos"),
        Shell(
            "helm install kuberay-operator kuberay/kuberay-operator --version 1.3.0 --wait",
            "Installing kuberay-operator",
        ),
        Shell(
            "helm install spark-operator spark-operator/spark-operator",
            "Installing spark-operator",
        ),
        Template("pvcs.yaml.template", "rewrite pvcs.yaml.template"),
        Shell("kubectl apply -f pvcs.yaml", "Apply pvcs"),
    ],
}
