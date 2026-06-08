FROM python:3.11-slim

RUN pip install --no-cache-dir \
      jupyterlab==4.2.1 \
      clickhouse-connect==0.7.19 \
      pandas==2.2.2 \
      matplotlib==3.9.0 \
      seaborn==0.13.2 \
      plotly==5.22.0

WORKDIR /home/jovyan/work
EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--allow-root", "--NotebookApp.token=", "--NotebookApp.password="]
