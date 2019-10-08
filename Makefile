
run-appliance:
	mvn clean package -pl transporter
	mvn clean package -pl consumer
	docker-compose up --build
	


