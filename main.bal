import ballerinax/kafka; // Import Kafka library for messaging
import ballerina/sql; // Import SQL module for database operations
import ballerinax/mysql; // Import MySQL library for database interaction
import ballerina/http; // Import HTTP module for creating HTTP services
import ballerina/uuid; // Import UUID library for generating unique IDs
import ballerinax/mysql.driver as _; // Import MySQL driver

// Define the structure of a shipment detail record
// This record holds all necessary details for a shipment
type ShipmentDetails record {
    string shipmentType; 
    string pickupLocation; 
    string deliveryLocation; 
    string preferredTimeSlot; 
    string firstName; 
    string lastName; 
    string contactNumber; 
    string trackingNumber; 
};

// Define the structure of a shipment confirmation record
// This record is used to confirm shipment details and update statuses
type ShipmentConfirmation record {
    string confirmationId; 
    string shipmentType; 
    string pickupLocation; 
    string deliveryLocation; 
    string estimatedDeliveryTime; 
    string status; 
};

// Kafka listener that listens to the 'confirmationShipment' topic
// Used to consume shipment confirmation messages
listener kafka:Listener logisticConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "logistic-delivery-group", 
    topics: "confirmationShipment" 
});

// HTTP service exposed at port 9090 to handle logistic requests
service /cosmic_logistics on new http:Listener(9090) {
    private final mysql:Client db; 
    private final kafka:Producer kafkaProducer; 

    // Initialize Kafka producer and MySQL client
    function init() returns error? {
        // Initialize Kafka producer for sending messages to Kafka topics
        self.kafkaProducer = check new(kafka:DEFAULT_URL);
        
        // Initialize MySQL client to connect to the logistics database
        self.db = check new("localhost", "root", "Kalitheni@11", "logistics_db", 3306);
    }