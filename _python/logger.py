
from getpass import getpass
from enum import IntEnum
from math import floor
from shutil import get_terminal_size
from threading import Thread
from time import sleep

class Level(IntEnum):
    TRACE = 0
    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4
    FATAL = 5
    INPUT = 6
    NONE = 7

class Console:
    cycling = False
    cr = False
    _min_lvl = Level.DEBUG
    _DEF_LVL = Level.DEBUG

    def _cycle(*args: tuple):
        
        lvl = Console._DEF_LVL
        if args and isinstance(args[0], Level):
            lvl = args[0]

        cycle_str = Console._gen_msg_str(*args)

        while Console.cycling:
            for c in [' |', ' /', ' -', ' \\']:
                if Console._is_loggable(lvl):
                    print(f"{cycle_str}{c}", end='\r')
                    sleep(.1)
                    if not Console.cr: Console.cr = True
                else:
                    Console.cycling = False
                    return

    def _gen_lvl_str(lvl: Level) -> str:
        lvl_str = '\t '
        if lvl == Level.TRACE:
            lvl_str = '\033[90m[TRACE]\033[0m\t '
        elif lvl == Level.DEBUG:
            lvl_str = '\033[97m[DEBUG]\033[0m\t '
        elif lvl == Level.INFO:
            lvl_str = '\033[97;46m [INFO]\033[0m\t '
        elif lvl == Level.WARN:
            lvl_str = '\033[43;30m [WARN]\033[0m\t '
        elif lvl == Level.ERROR:
            lvl_str = '\033[97;101m[ERROR]\033[0m\t '
        elif lvl == Level.FATAL:
            lvl_str = '\033[97;41m[FATAL]\033[0m\t '
        elif lvl == Level.INPUT:
            lvl_str = '\033[95m[INPUT]\033[0m\t '

        return lvl_str

    def _gen_msg_str(*args: tuple) -> str:
        msg_str = ''

        lvl = Console._DEF_LVL
        if args and isinstance(args[0], Level):
            lvl = args[0]
            args = args[1:]

        msg_str += Console._gen_lvl_str(lvl)
        msg_str += ''.join(str(msg).replace('\n', '\n       \t ') for msg in args)

        if args and isinstance(args[-1], str) and args[-1].endswith('\r'):
            Console.cr = True
        else:
            Console.cr = False

        return msg_str

    def _gen_bar_str(*args: tuple) -> str:
        bar_str = ''

        lvl = Console._DEF_LVL
        if args and isinstance(args[0], Level):
            lvl = args[0]
            current = args[1] if len(args) > 1 else 0
            total = args[2] if len(args) > 2 else 100
            width = args[3] if len(args) > 3 else 80
        else:
            current = args[0] if len(args) > 0 else 0
            total = args[1] if len(args) > 1 else 100
            width = args[2] if len(args) > 2 else 80

        current = min(current, total)

        bar_str += Console._gen_lvl_str(lvl)

        available_space = width
        completed_space = int(floor(float(current) / total * available_space))
        percent = round((current / total) * 100)

        bar_str += f"{percent}%".ljust(5, ' ')
        bar_str += f"{'■'*completed_space}{'='*(available_space-completed_space)} "
        bar_str += f"[ {current} / {total} ] "

        return bar_str


    def _is_loggable(lvl: Level) -> bool:
        if lvl.value >= Console._min_lvl.value:
            return True
        else:
            return False

    def set_level(lvl: Level):
        Console._min_lvl = lvl

    def log(*args: tuple, end: str ="\n\r"):
        lvl = Console._DEF_LVL
        if args and isinstance(args[0], Level): lvl = args[0]

        if Console._is_loggable(lvl):
            if args and ((isinstance(args[0], Level) and len(args) > 1) or (not isinstance(args[0], Level) and len(args) > 0)): 
                print('\r' + " " * (get_terminal_size().columns), end='\r')
            print(Console._gen_msg_str(*args), end=end)

    def inp(msg: str, isPrivate: bool = False, default_value: str = None):
        inp_str = Console._gen_msg_str(Level.INPUT, msg)
        if default_value: inp_str += f" [{default_value}]"
        inp_str += ": "

        Console.cr = True
        if isPrivate:
            inp_res = getpass(inp_str) or default_value
        else:
            inp_res = input(inp_str) or default_value
            Console.log(Level.TRACE, f"Provided input: {inp_res}")

        return inp_res
    
    def bar(*args: tuple):
        lvl = Console._DEF_LVL
        if args and isinstance(args[0], Level):
            lvl = args[0]

        if Console._is_loggable(lvl):
            # end = '\r'
            # if not Console.cr: end += '\n'

            print(Console._gen_bar_str(*args), end='\r')
            Console.cr = True

    thread = None

    def start_cycle(*args: tuple):
        Console.thread = Thread(target=Console._cycle, args=args)
        Console.cycling = True
        Console.thread.start()

    def end_cycle():
        Console.cycling = False
        Console.thread.join()
