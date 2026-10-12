import abc
import typing


class DataProcessor(abc.ABC):
    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def __init__(self) -> None:
        self.dictionary: dict[int, str] = {}
        self.contador: int = 0

    def output(self) -> tuple[int, str]:
        if not self.dictionary:
            raise ValueError("error - no more data")
        key = min(self.dictionary)
        value = self.dictionary.pop(key)
        return (key, value)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        return (
            isinstance(data, (int, float))
            or (isinstance(data, list)
                and all(isinstance(x, (int, float)) for x in data)))

    def ingest(self, data: typing.Any) -> None:
        numbers = data if isinstance(data, list) else [data]
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        for x in numbers:
            self.dictionary[self.contador] = str(x)
            self.contador += 1


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        return (
            isinstance(data, str)
            or (isinstance(data, list)
                and all(isinstance(x, (str)) for x in data)))

    def ingest(self, data: typing.Any) -> None:
        strings = data if isinstance(data, list) else [data]
        if not self.validate(data):
            raise ValueError("Improper text data")
        for x in strings:
            self.dictionary[self.contador] = x
            self.contador += 1


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            return all((isinstance(k, str)
                        and isinstance(v, str)) for k, v in data.items())
        if isinstance(data, list):
            return (
                all(
                    isinstance(element, dict)
                    and all((isinstance(k, str)
                             and isinstance(v, str))
                            for k, v in element.items())
                    for element in data))
        return False

    def ingest(self, data: typing.Any) -> None:
        strings = data if isinstance(data, list) else [data]
        if not self.validate(data):
            raise ValueError("Improper log data")
        for x in strings:
            self.dictionary[self.contador] = ": ".join(x.values())
            self.contador += 1


if __name__ == '__main__':
    numbers = NumericProcessor()
    text = TextProcessor()
    logs = LogProcessor()
    print("=== Code Nexus - Data Processor ===")
    print()
    print("Testing Numeric Processor...")
    print(f" Trying to validate input '42': {numbers.validate(42)}")
    print(f" Trying to validate input 'Hello': {numbers.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numbers.ingest("foo")
    except ValueError as e:
        print(f" Got exception: {e}")
    array = [1, 2, 3, 4, 5]
    numbers.ingest(array)
    print(f" Processing data: {array}")
    print(" Extracting 3 values...")
    for _ in range(3):
        try:
            k, v = numbers.output()
            print(f" Numeric value {k}: {v}")
        except ValueError as e:
            print(f" Numeric value {e}")
    print()
    print("Testing Text Processor...")
    print(f" Trying to validate input '42': {text.validate(42)}")
    words = ['Hello', 'Nexus', 'World']
    text.ingest(words)
    print(f" Processing data: {words}")
    print(" Extracting 1 value...")
    for _ in range(1):
        try:
            k, v = text.output()
            print(f" Text value {k}: {v}")
        except ValueError as e:
            print(f" Text value {e}")
    print()
    print("Testing Log Processor...")
    print(f" Trying to validate input 'Hello': {logs.validate('Hello')}")
    data_log = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    logs.ingest(data_log)
    print(f" Processing data: {data_log}")
    print(" Extracting 2 values...")
    for _ in range(2):
        try:
            k, v = logs.output()
            print(f" Log entry {k}: {v}")
        except ValueError as e:
            print(f" Log entry {e}")
