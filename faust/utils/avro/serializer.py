import avro
import avro.io

try:
    from fastavro.reader import read_data
    HAS_FAST = True
except ImportError:  # prgama: no cover
    HAS_FAST = False
