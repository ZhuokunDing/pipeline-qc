from . import MissingError, logger, virtual as V


def find_same_session_stack(scan: dict):
    stack = V.experiment.Stack & scan
    if len(stack) == 1:
        return stack.fetch1("KEY")
    elif len(stack) == 0:
        raise MissingError(
            f"Did not find any stack in the same session for {scan}."
        )
    else:
        raise ValueError(f"Found more than one stack for {scan}.")

def find_stack(scan: dict):
    stack_key = find_same_session_stack(scan)
    logger.info(f"Found same session stack for {scan}: {stack_key}")
    # TODO: add logic to find stack in nearest session if no stack in same session
    return stack_key
