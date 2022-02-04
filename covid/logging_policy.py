import argparse
import contextlib
import logging
import re
import sys
import traceback
import warnings

argument_parser = argparse.ArgumentParser(add_help=False)
argument_group = argument_parser.add_argument_group("logging")
argument_group.add_argument("--quiet", action="store_true")
argument_group.add_argument("--debug", action="store_true")
argument_group.add_argument("--debug_http", action="store_true")


@contextlib.contextmanager
def collecting_warnings(allow_regex=None):
    collected = []
    regex = allow_regex and re.compile(allow_regex)
    saved = warnings.showwarning

    def handler(message, category, filename, lineno, file, line):
        text = str(message).strip()
        if regex and regex.fullmatch(text):
            logging.info(f"🟡   {text}")
        else:
            collected.append(text)
            saved(message, category, filename, lineno, file, line)
            traceback.print_stack(file=sys.stdout)
            print()

    warnings.showwarning = handler
    try:
        yield collected
    finally:
        warnings.showwarning = saved


class _LogFormatter(logging.Formatter):
    def format(self, record):
        m = record.getMessage()
        ml = m.lstrip()
        out = ml.rstrip()
        pre, post = m[: len(m) - len(ml)], ml[len(out) :]
        if record.name != "root":
            out = f"{record.name}: {out}"
        if record.levelno < logging.INFO:
            out = f"🕸  {out}"
        elif record.levelno >= logging.CRITICAL:
            out = f"💥  {out}"
        elif record.levelno >= logging.ERROR:
            out = f"🔥  {out}"
        elif record.levelno >= logging.WARNING:
            out = f"⚠️   {out}"
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            out = f"{out.strip()}\n{record.exc_text}"
        if record.stack_info:
            out = f"{out.strip()}\n{record.stack_info}"
        return pre + out.strip() + post


def _sys_exception_hook(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt):
        logging.critical("*** KeyboardInterrupt (^C)! ***")
    else:
        exc_info = (exc_type, exc_value, exc_tb)
        logging.critical("Uncaught exception", exc_info=exc_info)


def _warning_hook(message, category, filename, lineno, file, line):
    logging.warn(str(message).strip())


# Initialize on import.
_log_handler = logging.StreamHandler(stream=sys.stdout)
_log_handler.setFormatter(_LogFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_log_handler])
sys.excepthook = _sys_exception_hook
warnings.showwarning = _warning_hook
