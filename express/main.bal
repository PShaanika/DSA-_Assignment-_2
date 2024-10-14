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


// This record is used to confirm shipment details and update statuses
type ShipmentConfirmation record {
    string confirmationId; 
    string shipmentType; 
    string pickupLocation; 
    string deliveryLocation; 
    string estimatedDeliveryTime; 
    string status; 
};


// This connection allows you to read and write shipment data, customer data, etc.
// It connects to the local MySQL database using root as the username and password,
// and the database is located on port 3306.
mysql:Client logisticsDb = check new("localhost", "root", "Kalitheni@11", "logistics_db", 3306);


// This listener listens to messages on the 'standard-delivery' topic in the Kafka broker.
// It belongs to the 'standard-delivery-group' consumer group.
listener kafka:Listener regularShipmentConsumer = check new(kafka:DEFAULT_URL, {
    groupId: "standard-delivery-group",  
    topics: "standard-delivery"
});


// This producer sends shipment confirmations to the 'confirmationShipment' Kafka topic.
kafka:Producer shipmentNotifier = check new(kafka:DEFAULT_URL);


function estimateShippingDuration(string pickupLocation, string deliveryLocation) returns string {
    // Static estimation of shipping duration.
    // Replace with dynamic calculation as needed.
    string estimatedTime = "15 - 30 lunar cycles";
    return estimatedTime;
}

// Kafka consumer service to handle incoming shipment requests
service on regularShipmentConsumer {
    //
    remote function onConsumerRecord(ShipmentDetails[] shipmentRequests) returns error? {
        
        foreach ShipmentDetails request in shipmentRequests {
        
            string deliveryEstimate = estimateShippingDuration(request.pickupLocation, request.deliveryLocation);
            
            // Prepare shipment confirmation details
            ShipmentConfirmation confirmation = {
                confirmationId: request.trackingNumber, 
                shipmentType: request.shipmentType, 
                pickupLocation: request.pickupLocation, 
                deliveryLocation: request.deliveryLocation, 
                estimatedDeliveryTime: deliveryEstimate, 
                status: "Quantum Entangled" 
            };
            
            // Send the shipment confirmation to the 'confirmationShipment' topic in Kafka.
            // This notifies other services or systems that the shipment has been confirmed
            // and provides the estimated delivery timeframe.
            check shipmentNotifier->send({topic: "confirmationShipment", value: confirmation});
            
            // Log the confirmation that the shipment confirmation was sent.
            io:println("Intergalactic shipment confirmation dispatched for tracking number: " + request.trackingNumber);
        }
    }
}
