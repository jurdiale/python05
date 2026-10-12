import abc
import typing


class DataProcessor(abc.ABC):
    nombre = "Data Processor"

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
            raise ValueError("Error - no more data")
        key = min(self.dictionary)
        value = self.dictionary.pop(key)
        return (key, value)


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class PluginCSV:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        printed = [v for k, v in data]
        print("CSV Output: ")
        print(f"{','.join(printed)}")


class PluginJSON:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pairs = [f'"item_{k}": "{v}"' for k, v in data]
        print("JSON Output: ")
        print(f"{'{' + ', '.join(pairs) + '}'}")


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            for processor in self.processors:
                if processor.validate(element):
                    processor.ingest(element)
                    break
            else:
                print(
                    "DataStream error - Can't process element"
                    f" in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
        else:
            for processor in self.processors:
                print(
                    f"{processor.nombre}: total {processor.contador}"
                    f" items processed, remaining {len(processor.dictionary)}"
                    " on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self.processors:
            data: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    data.append(processor.output())
                except ValueError:
                    break
            plugin.process_output(data)


class NumericProcessor(DataProcessor):
    nombre = "Numeric Processor"

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
    nombre = "Text Processor"

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
    nombre = "Log Processor"

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
    print("=== Code Nexus - Data Pipeline ===")
    print()
    print("Initialize Data Stream...")
    datas = DataStream()
    datas.print_processors_stats()
    print()
    print("Registering Processors")
    number = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    datas.register_processor(number)
    datas.register_processor(text)
    datas.register_processor(log)
    print()
    batch = ['Hello world', [3.14, -1, 2.71],
             [{'log_level': 'WARNING',
               'log_message': 'Telnet access! Use ssh instead'},
              {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
             42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {batch}")
    datas.process_stream(batch)
    print()
    datas.print_processors_stats()
    print()
    print("Send 3 processed data from each processor to a CSV plugin:")
    csv = PluginCSV
    json = PluginJSON
    datas.output_pipeline(3, csv())
    print()
    datas.print_processors_stats()
    batch = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
             [{'log_level': 'ERROR', 'log_message': '500 server crash'},
              {'log_level': 'NOTICE',
               'log_message': 'Certificate expires in 10 days'}],
             [32, 42, 64, 84, 128, 168], 'World hello']
    print(f"Send another batch of data: {batch}")
    print()
    datas.process_stream(batch)
    datas.print_processors_stats()
    print("Send 5 processed data from each processor to a JSON plugin:")
    datas.output_pipeline(5, json())
    print()
    datas.print_processors_stats()
