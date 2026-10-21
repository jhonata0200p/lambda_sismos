"""
Servidor Trigger HTTP para Apache Spark Batch.
Permite que Apache Airflow (o cualquier orquestador) dispare la ejecución
del pipeline 'spark_batch_etl.py' de forma síncrona y segura dentro de la red Docker.
"""

import http.server
import json
import logging
import subprocess
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SparkTriggerServer] %(message)s"
)

PORT = 5050
SPARK_SUBMIT_BIN = "/opt/spark/bin/spark-submit"
SPARK_SCRIPT_PATH = "/opt/spark/apps/spark_batch_etl.py"


class SparkTriggerHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        logging.info(f"{self.address_string()} - {format % args}")

    def do_GET(self):
        if self.path in ["/", "/health", "/status"]:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "HEALTHY",
                "service": "Spark Batch Trigger Service",
                "script": SPARK_SCRIPT_PATH
            }).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path in ["/trigger", "/run-batch", "/ejecutar-batch"]:
            logging.info("==================================================")
            logging.info(">>> Solicitud de disparo recibida desde Airflow <<<")
            logging.info("==================================================")
            try:
                cmd = [SPARK_SUBMIT_BIN, SPARK_SCRIPT_PATH]
                logging.info(f"Iniciando comando: {' '.join(cmd)}")
                
                proc = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=600  # 10 minutos máximo
                )
                
                output = proc.stdout or ""
                returncode = proc.returncode
                
                if returncode == 0:
                    logging.info(">>> spark_batch_etl.py finalizó con ÉXITO (código 0).")
                    self.send_response(200)
                else:
                    logging.error(f">>> spark_batch_etl.py falló con código {returncode}.")
                    self.send_response(500)
                    
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                
                response_payload = {
                    "status": "SUCCESS" if returncode == 0 else "FAILED",
                    "returncode": returncode,
                    "output": output[-6000:] if len(output) > 6000 else output
                }
                self.wfile.write(json.dumps(response_payload).encode("utf-8"))

            except subprocess.TimeoutExpired:
                logging.error("Tiempo de ejecución excedido (Timeout 600s)")
                self.send_response(504)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "TIMEOUT",
                    "error": "El job de Spark excedió el tiempo límite de 10 minutos."
                }).encode("utf-8"))

            except Exception as e:
                logging.error(f"Error ejecutando Spark Batch: {e}")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "ERROR",
                    "error": str(e)
                }).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()


def run():
    server_address = ("0.0.0.0", PORT)
    httpd = http.server.HTTPServer(server_address, SparkTriggerHandler)
    logging.info(f"Iniciando Spark Trigger Server en 0.0.0.0:{PORT}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info("Servidor detenido.")


if __name__ == "__main__":
    run()
