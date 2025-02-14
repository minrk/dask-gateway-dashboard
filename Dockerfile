FROM python:3.12-slim
COPY requirements.txt /tmp/
RUN pip install --no-cache -r /tmp/requirements.txt
COPY / /srv/
WORKDIR /srv/
EXPOSE 8000
CMD ["fastapi", "run", "dask_labextension_dashboard", "--port", "8000"]
