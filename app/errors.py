"""业务校验错误：消息可直接展示给用户。"""


class ValidationError(Exception):
    """输入文件不符合 JSONL 约定（结构、大小、id 唯一性等）。"""

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
