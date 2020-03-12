# Generate the CA Key and Certificate
openssl req -x509 -sha256 -newkey rsa:4096 -keyout ca.key -out ca.crt -days 356 -nodes -subj '/CN=Kurbisio Cert Authority'
# Generate the Server Key, and Certificate and Sign with the CA Certificate
openssl req -new -newkey rsa:4096 -keyout server.key -out server.csr -nodes -subj '/CN=kurbis.io'
openssl x509 -req -sha256 -days 365 -in server.csr -CA ca.crt -CAkey ca.key -set_serial 01 -out server.crt
# Generate the Client Key, and Certificate and Sign with the CA Certificate (this is done by service itself)
openssl req -new -newkey rsa:4096 -keyout client.key -out client.csr -nodes -subj '/CN={device_id}'
openssl x509 -req -sha256 -days 365 -in client.csr -CA ca.crt -CAkey ca.key -set_serial 02 -out client.crt


start database:
docker run  --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=docker -d postgres


log into database:
docker exec -it some-postgres psql -U postgres


start streaming server:
docker run -p 4223:4223 -p 8223:8223 nats-streaming nats-streaming-server -p 4223 -m 8223

