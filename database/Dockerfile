# Używamy oficjalnego, lekkiego obrazu Pythona
FROM python:3.11-slim

# Ustawiamy katalog roboczy wewnątrz kontenera
WORKDIR /app

# Instalujemy zależności systemowe (często wymagane przez biblioteki łączące się z bazą i Kafką)
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Kopiujemy plik z listą pakietów i instalujemy je
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopiujemy całą resztę skryptów
COPY . .

# Komenda startowa: uruchamia konsumenta w tle (&) oraz predykcję na pierwszym planie
CMD ["sh", "-c", "python consumer.py & python interferance.py"]