# python05 — Code Nexus: Polymorphic Data Streams

## 1. Qué hace el programa

Tres ejercicios que se construyen uno sobre otro:

| Ejercicio | Fichero | Qué añade |
|---|---|---|
| ex0 | `data_processor.py` | Clase abstracta `DataProcessor` y tres hijas: `NumericProcessor`, `TextProcessor`, `LogProcessor` |
| ex1 | `data_stream.py` | `DataStream`: recibe una lista mezclada y reparte cada elemento al procesador que lo acepta |
| ex2 | `data_pipeline.py` | Salida por plugins (`ExportPlugin` como `Protocol`): `PluginCSV` y `PluginJSON` |

**Flujo:** `DataStream.process_stream` recorre la lista y, para cada elemento, pregunta a cada procesador `validate(elemento)`. El primero que dice `True` hace `ingest(elemento)`. Si ninguno lo acepta, imprime `DataStream error - Can't process element in stream: ...`.

**Cada procesador** guarda lo ingerido como texto en un diccionario `{rango: texto}`. `contador` sube siempre (es el rango y el total procesado). `output()` saca el elemento de menor rango y lo borra, así que el *total* no baja pero los *restantes* sí. Por eso en el JSON salen claves `item_3`, `item_4`...: el rango no se reinicia.

**ex2:** `output_pipeline(nb, plugin)` saca hasta `nb` elementos de cada procesador y se los pasa al plugin, que solo necesita tener un método `process_output`.

## 2. Conceptos que hay que saber

1. **Clase abstracta (`ABC`)**: plantilla que no se puede instanciar. `DataProcessor()` da `TypeError` porque tiene métodos abstractos sin implementar.
2. **`@abstractmethod`**: obliga a las hijas a implementar `validate` e `ingest`. `output` es normal: se hereda tal cual.
3. **Herencia y `__init__`**: las hijas no definen `__init__`, usan el de la madre (`dictionary` y `contador`).
4. **Sobrescritura (overriding)**: cada hija reescribe `validate` e `ingest` con su lógica.
5. **Polimorfismo de subtipos**: `DataStream` llama a `processor.validate(...)` sin saber si es Numeric, Text o Log. Cada objeto responde a su manera.
6. **Atributo de clase `nombre`**: lo define cada hija y se usa al imprimir estadísticas.
7. **`typing.Any`** en la madre y tipos concretos en las hijas: la madre no sabe qué datos llegarán; la hija sí. Con `Any` mypy permite estrechar el tipo en la hija.
8. **`Protocol` y duck typing**: `PluginCSV` y `PluginJSON` no heredan de `ExportPlugin`. Valen porque tienen el método con la firma correcta. Es tipado estructural ("si anda como pato...").
9. **Excepciones**: `ingest` lanza `ValueError` si se le da un dato no válido; `output` lo lanza si está vacío. El `try/except` en el main lo captura.
10. **`isinstance` con tuplas**, `all(...)` con generadores y `for ... else` (el `else` del `for` solo se ejecuta si no hubo `break`).
11. **Varianza de listas (mypy)**: `list[int]` no es `list[int | float]`. Por eso en ex0 la variable se anota `array: list[int | float]`.

## 3. Cambios aplicados respecto a lo que había en el repo

- **Firmas de `ingest` en las hijas** reflejan los tipos aceptados, como pide el subject ("The overriding ingest method signature must reflect the accepted types"):
  - Numeric: `int | float | list[int | float]`
  - Text: `str | list[str]`
  - Log: `dict[str, str] | list[dict[str, str]]`
  - Con esto `numbers.ingest("foo")` da **un aviso de mypy a propósito** (el subject lo dice: "This will leave you with a mypy warning, on purpose"). Antes con `Any` no salía ninguno.
- **ex2:** `"CSV Output:"` y `"JSON Output:"` sin espacio final, igual que el subject.
- La salida de los tres programas es idéntica a la de antes salvo ese espacio; flake8 limpio; mypy limpio salvo el aviso intencionado de ex0.

## 4. Defensa

### Explicación en 1 minuto
"`DataProcessor` es una clase abstracta con dos métodos abstractos, `validate` e `ingest`, y uno normal, `output`. Las tres hijas sobrescriben los abstractos. `DataStream` guarda una lista de `DataProcessor` y, para cada elemento, usa `validate` para ver quién lo acepta y `ingest` para guardarlo. No sabe qué clase concreta es cada uno: eso es polimorfismo. En ex2 la salida son plugins que solo necesitan un método `process_output`, sin heredar de nada: duck typing con `Protocol`."

### Preguntas probables
- **¿Qué pasa si hago `DataProcessor()`?** `TypeError`: tiene métodos abstractos.
- **¿Por qué `validate` e `ingest` son abstractos y `output` no?** Validar e ingerir dependen del tipo de dato; sacar el más antiguo es igual para todos.
- **¿Por qué `Any` en la clase madre?** No sabe qué tipo llegará; cada hija lo concreta.
- **¿Cómo permite el polimorfismo manejar tipos distintos?** `DataStream` solo conoce la interfaz (`validate`, `ingest`, `output`). Añadir un procesador nuevo no obliga a tocar `DataStream`.
- **¿Qué ventaja tiene?** Se extiende sin modificar código existente y el código se reutiliza.
- **¿Diferencia entre `ABC` y `Protocol`?** `ABC` exige herencia explícita; `Protocol` solo exige que el objeto tenga los métodos.
- **¿Por qué el orden de registro importa?** Cada elemento va al primer procesador que lo valida. Un `[]` vacío lo aceptaría el primero (todos los `all(...)` sobre lista vacía dan `True`).
- **Extender con un tipo nuevo** (por ejemplo, `BoolProcessor`): nueva hija de `DataProcessor` con `nombre`, `validate` e `ingest`; registrarla con `register_processor`. `DataStream` no cambia.
- **Cambiar el comportamiento:** por ejemplo, `output` que saque el más reciente (LIFO) → `max(self.dictionary)` en vez de `min`.
- **¿Por qué `contador` no baja al hacer `output`?** Es el rango y el total procesado; lo que baja es `len(dictionary)`.

### Puntos débiles que conviene conocer
- `True`/`False` pasan como numéricos (`bool` es subclase de `int`).
- Una lista vacía es válida para los tres procesadores.
- El primero que valida se queda con el dato: un `dict` vacío lo acepta `LogProcessor`.
- Los plugins no escapan comas o comillas dentro del texto: el JSON/CSV puede quedar inválido con datos raros. Es una limitación conocida, el subject no pide más.
- ex1 y ex2 repiten las clases de ex0 porque cada ejercicio se entrega solo (un fichero por directorio).

## 5. Comprobaciones antes de entregar
```bash
cd ex0 && flake8 data_processor.py && mypy data_processor.py   # 1 aviso a propósito (ingest "foo")
cd ../ex1 && flake8 data_stream.py && mypy data_stream.py
cd ../ex2 && flake8 data_pipeline.py && mypy data_pipeline.py
```
Entregar solo `ex0/data_processor.py`, `ex1/data_stream.py` y `ex2/data_pipeline.py`.
