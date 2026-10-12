from datetime import datetime


class Logger:
    RESET: str = "\033[0m"
    BLUE: str = "\033[94m"
    RED: str = "\033[91m"
    CYAN: str = "\033[96m"
    YELLOW: str = "\033[93m"

    @staticmethod
    def _timestamp() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def Info(self, message: str) -> None:
        print(f"{Logger.BLUE}[{Logger._timestamp()}] [INFO] {message}{Logger.RESET}", flush=True)

    def Error(self, message: str, error: BaseException | type[BaseException]) -> None:
        print(f"{Logger.RED}[{Logger._timestamp()}] [ERROR] {message}{error}{Logger.RESET}", flush=True)

    def Debug(self, message: str) -> None:
        print(f"{Logger.CYAN}[{Logger._timestamp()}] [DEBUG] {message}{Logger.RESET}", flush=True)

    def Warning(self, message: str) -> None:
        print(f"{Logger.YELLOW}[{Logger._timestamp()}] [WARNING] {message}{Logger.RESET}", flush=True)
