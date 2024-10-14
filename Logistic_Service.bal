import ballerinax/kafka; // Import Kafka library for messaging
import ballerina/sql; // Import SQL module for database operations
import ballerinax/mysql; // Import MySQL library for database interaction
import ballerina/http; // Import HTTP module for creating HTTP services
import ballerina/uuid; // Import UUID library for generating unique IDs
import ballerinax/mysql.driver as _; // Import MySQL driver

// Define the structure of a shipment detail record
// This record holds all necessary details for a shipment
type ShipmentDetails record {
    string shipmentType; // Type of shipment (e.g., standard, express)
    string pickupLocation; // Pickup location
    string deliveryLocation; // Delivery location
    string preferredTimeSlot; // Preferred delivery time slot
    string firstName; // First name of the customer
    string lastName; // Last name of the customer
    string contactNumber; // Contact number of the customer
    string trackingNumber; // Unique tracking number for the shipment
};

// Define the structure of a shipment confirmation record
// This record is used to confirm shipment details and update statuses
type ShipmentConfirmation record {
    string confirmationId; // Confirmation ID, same as the tracking number
    string shipmentType; // Type of shipment confirmed
    string pickupLocation; // Pickup location of the shipment
    string deliveryLocation; // Delivery location of the shipment
    string estimatedDeliveryTime; // Estimated time of delivery
    string status; // Status of the shipment (e.g., 'Confirmed')
};

// Kafka listener that listens to the 'confirmationShipment' topic
// Used to consume shipment confirmation messages
listener kafka:Listener logisticConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "logistic-delivery-group", // Consumer group for logistic deliveries
    topics: "confirmationShipment" // Topic this service listens to
});

// HTTP service exposed at port 9090 to handle logistic requests
service /cosmic_logistics on new http:Listener(9090) {
    private final mysql:Client db; // MySQL client to interact with the database
    private final kafka:Producer kafkaProducer; // Kafka producer to send messages

    // Initialize Kafka producer and MySQL client
    function init() returns error? {
        // Initialize Kafka producer for sending messages to Kafka topics
        self.kafkaProducer = check new(kafka:DEFAULT_URL);
        
        // Initialize MySQL client to connect to the logistics database
        self.db = check new("localhost", "root", "Kalitheni@11", "logistics_db", 3306);
    }

    // Resource to handle POST requests for sending packages
    // Endpoint: http://localhost:9090/cosmic_logistics/sendPackage
    resource function post sendPackage(ShipmentDetails req) returns string|error? {
        // Generate a unique tracking number for the shipment
        req.trackingNumber = uuid:createType1AsString();

        // Insert new customer data into the Customers table
        sql:ParameterizedQuery insertCustomerQuery = `
            INSERT INTO Customers (first_name, last_name, contact_number) 
            VALUES (${req.firstName}, ${req.lastName}, ${req.contactNumber})`;
        // Execute the insert query and handle any potential errors
        sql:ExecutionResult _ = check self.db->execute(insertCustomerQuery);

        // Insert shipment details into the Shipments table
        sql:ParameterizedQuery insertShipmentQuery = `
            INSERT INTO Shipments 
            (shipment_type, pickup_location, delivery_location, preferred_time_slot, tracking_number) 
            VALUES 
            (${req.shipmentType}, ${req.pickupLocation}, ${req.deliveryLocation}, ${req.preferredTimeSlot}, ${req.trackingNumber})`;
        // Execute the insert query and handle any potential errors
        sql:ExecutionResult _ = check self.db->execute(insertShipmentQuery);

        // Produce shipment details to Kafka with the shipment type as the topic
        // This allows other services to consume shipment details based on type
        check self.kafkaProducer->send({topic: req.shipmentType, value: req});

        return string `${req.firstName} ${req.lastName} shipment ${req.trackingNumber} has been confirmed.`;
    }
}

// Service that processes messages from the 'confirmationShipment' Kafka topic
service on logisticConsumer {
    private final mysql:Client db; // MySQL client to interact with the database

    // Initialize MySQL client
    function init() returns error? {
        // Initialize MySQL client to connect to the logistics database
        self.db = check new("localhost", "root", "Kalitheni@11", "logistics_db", 3306);
    }

    // Function to process consumed shipment confirmations
    // Automatically triggered when messages are consumed from the Kafka topic
    remote function onConsumerRecord(ShipmentConfirmation[] confirmations) returns error? {
        // Loop through each shipment confirmation received
        foreach ShipmentConfirmation confirmation in confirmations {
            // Update the shipment status in the database to 'confirmed'
            sql:ParameterizedQuery updateQuery = `
                UPDATE Shipments 
                SET status = "confirmed", estimated_delivery_time = ${confirmation.estimatedDeliveryTime} 
                WHERE tracking_number = ${confirmation.confirmationId}`;
            // Execute the update query and handle any potential errors
            sql:ExecutionResult _ = check self.db->execute(updateQuery);
        }
    }
}
