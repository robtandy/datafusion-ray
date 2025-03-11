from collections import namedtuple
import subprocess
import jinja2
import click
import os

Shell = namedtuple("Shell", ["cmd", "desc"])
Template = namedtuple("Template", ["path", "desc"])
ChangeDir = namedtuple("ChangeDir", ["path", "desc"])

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
    "generate_data": [
        Shell(
            "git clone https://github.com/apache/datafusion-benchmarks/",
            "Cloning apache/datafusion-benchmarks",
        ),
        ChangeDir("datafusion-benchmarks", "Change to repo dir"),
        Shell("pip install -r requirements.txt", "install repo requirements"),
        ChangeDir("tpch", "Change to tpch dir"),
        Shell("mkdir {{data_path}}/sf{{scale_factor}}", "make data dir"),
        Shell("rm -f data", "remove existing symlink if any"),
        Shell("ln -s {{data_path}}/sf{{scale_factor}} data", "symlink data dir"),
        Shell(
            "python tpchgen.py generate --sf {{scale_factor}} --partitions {{partitions}}",
            "generate the data",
        ),
        Shell(
            "python tpchgen.py convert --sf {{scale_factor}} --partitions {{partitions}}",
            "convert the data to parquet",
        ),
    ],
}


class Runner:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.cwd = os.getcwd()

    def set_cwd(self, path):
        self.cwd = path

    def run_commands(
        self,
        commands: list[dict[str, str]],
        substitutions: dict[str, str] | None = None,
    ):
        for command in commands:
            match (self.dry_run, command):
                case (False, Shell(cmd, desc)):
                    click.secho(f"{desc} ...")
                    return_code, stdout, stderr = self.run_shell_command(cmd)
                    if return_code == 0:
                        click.secho(f"    {stdout}", fg="green")
                    else:
                        click.secho(f"    {stderr}", fg="red")
                        exit(1)

                case (True, Shell(cmd, desc)):
                    click.secho(f"[dry run] {desc} ...")
                    click.secho(f"    {cmd}", fg="yellow")

                case (False, Template(path, desc)):
                    self.process_template(path, ".", substitutions)

                case (True, Template(path, desc)):
                    click.secho(f"[dry run] {desc} ...")
                    click.secho(f"    {path} subs:{substitutions}", fg="yellow")

    def run_shell_command(self, command):
        process = subprocess.Popen(
            command,
            shell=True,
            cwd=self.cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = process.communicate()
        return process.returncode, stdout.decode(), stderr.decode()

    def process_template(
        self, template_path: str, output_path: str, substitutions: dict[str, str] | None
    ):
        template = jinja2.Template(open(template_path).read())

        with open(output_path, "w") as f:
            f.write(template.render(substitutions))
