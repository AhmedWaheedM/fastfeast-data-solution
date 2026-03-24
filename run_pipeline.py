import sys, argparse, subprocess
from time import sleep
from rich.console import Console
from rich.text import Text
from rich.panel import Panel
from rich import print


def run(cmd):
    subprocess.run(cmd, text=True)


def main():
    console = Console()
    banner = Text(justify="center")
    banner.append("\n", style="bold cyan")
    banner.append("     ███████╗ █████╗ ███████╗████████╗███████╗███████╗ █████╗ ███████╗████████╗\n", style="bold cyan")
    banner.append("     ██╔════╝██╔══██╗██╔════╝╚══██╔══╝██╔════╝██╔════╝██╔══██╗██╔════╝╚══██╔══╝\n", style="bold cyan")
    banner.append("  █████╗  ███████║███████╗   ██║   █████╗  █████╗  ███████║███████╗   ██║   \n", style="bold cyan")
    banner.append("  ██╔══╝  ██╔══██║╚════██║   ██║   ██╔══╝  ██╔══╝  ██╔══██║╚════██║   ██║   \n", style="bold cyan")
    banner.append("  ██║     ██║  ██║███████║   ██║   ██║     ███████╗██║  ██║███████║   ██║   \n", style="bold cyan")
    banner.append("  ╚═╝     ╚═╝  ╚═╝╚══════╝   ╚═╝   ╚═╝     ╚══════╝╚═╝  ╚═╝╚══════╝   ╚═╝   \n", style="bold cyan")
    console.print(Panel(banner, subtitle="[dim]Data Pipeline v1.0[/dim]", border_style="cyan"))
    sleep(2)  

    p = argparse.ArgumentParser()
    p.add_argument("--date",  required=True)
    p.add_argument("--mode",  default="all", choices=["all", "batch", "stream"])
    p.add_argument("--hour",  type=int)
    p.add_argument("--hours", type=int, nargs="+", default=[8,10,12,14,16,18,20])
    a = p.parse_args()
    py = sys.executable

    if a.mode in ("all", "batch"):
        run([py, "-m", "pipeline.batch_processing", "--date", a.date])

    if a.mode in ("all", "stream"):
        for h in ([a.hour] if a.hour else a.hours):
            run([py, "-m", "pipeline.stream_processing","--date", a.date, "--hour", str(h)])


if __name__ == "__main__":
    main()
