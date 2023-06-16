from pathlib import Path

# Заменил все записи в файлы на эту функцию для проверки пути
def write_to_file(filepath:str, text:str) -> bool:
    output_file = Path(filepath)
    output_file.parent.mkdir(exist_ok=True, parents=True)
    output_file.write_text(text)