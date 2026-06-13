FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--theme.base=light", \
     "--theme.primaryColor=#2563EB", \
     "--theme.backgroundColor=#F8FAFC", \
     "--theme.secondaryBackgroundColor=#EFF6FF", \
     "--theme.textColor=#1E293B"]
