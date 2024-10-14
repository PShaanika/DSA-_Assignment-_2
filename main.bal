import ballerinax/kafka; 
import ballerina/io; 
import ballerinax/mysql; 
import ballerinax/mysql.driver as _; 

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

// Initialize a MySQL client to interact with the 'logistics_db'.
// and the database is located on port 3306.
mysql:Client db = check new("localhost", "root", "Kalitheni@11", "logistics_db", 3306);

// Kafka Listener:
// This listener listens to messages on the 'standard-delivery' topic in the Kafka broker.
// It belongs to the 'standard-delivery-group' group. Kafka is used for message-driven architecture,
// where different parts of the system communicate asynchronously via messages.
listener kafka:Listener standardConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "standard-delivery-group",  // Kafka consumer group ID.
    topics: "standard-delivery" // Topic this service listens to for standard delivery shipment requests.
});

// Kafka Producer:
// This producer sends messages related to shipment confirmations back to a specific Kafka topic.
// The messages are produced to the 'confirmationShipment' topic to notify other systems
// or services that the shipment has been confirmed.
kafka:Producer confirmationProducer = check new(kafka:DEFAULT_URL);

// Function to estimate delivery duration based on pickup and delivery locations
// This function currently returns a fixed value. It can be enhanced to use more sophisticated logic.
function estimateShippingDuration(string pickupLocation, string deliveryLocation) returns string {
    // Static estimation of delivery time.
    // Replace with dynamic calculation as needed.
    string estimatedTime = "2-3 days";
    return estimatedTime;
}

// Kafka consumer service to handle incoming shipment requests
service on standardConsumer {
    // This function is triggered when new shipment details are consumed from the Kafka 'standard-delivery' topic.
    // It processes each shipment request, calculates the delivery time, and sends a confirmation.
    remote function onConsumerRecord(ShipmentDetails[] shipmentRequests) returns error? {
        // Loop through each shipment request received in the Kafka message.
        foreach ShipmentDetails request in shipmentRequests {
            // Calculate the estimated delivery time based on pickup and delivery locations.
            string deliveryEstimate = estimateShippingDuration(request.pickupLocation, request.deliveryLocation);
            
            // Prepare shipment confirmation details
            ShipmentConfirmation confirmation = {
                confirmationId: request.trackingNumber, // Use the tracking number as the confirmation ID.
                shipmentType: request.shipmentType, // Set the type of shipment (e.g., 'standard').
                pickupLocation: request.pickupLocation, // Set the pickup location.
                deliveryLocation: request.deliveryLocation, // Set the delivery location.
                estimatedDeliveryTime: deliveryEstimate, // Set the calculated delivery time.
                status: "Confirmed" // Set the status to 'Confirmed' for the shipment.
            };
            
            // Send the shipment confirmation to the 'confirmationShipment' topic in Kafka.
            // This notifies other services or systems that the shipment has been confirmed
            check confirmationProducer->send({topic: "confirmationShipment", value: confirmation});

            // Log the confirmation that the shipment confirmation was sent.
            io:println("Shipment confirmation sent for tracking number: " + request.trackingNumber);
        }
    }
}
