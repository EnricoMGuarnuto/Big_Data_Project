README – Foot Traffic Kafka Producer

Overview
--------
This Kafka producer simulates foot traffic in a grocery store, generating synthetic customer visit events based on the day of the week and time of day. Events are sent to a Kafka topic in real time, using realistic behavioral probabilities derived from consumer research.

Functionality
-------------
- Publishes messages to a Kafka topic (default: foot_traffic).
- Each message includes:
  - customer_id (UUID)
  - entry_time and exit_time
  - trip_duration_minutes
  - weekday
  - time_slot
- Probabilities of customer entry vary by weekday and time slot.
- Configurable sleep interval between messages via the SLEEP environment variable (default = 1 second).

Time Slot Structure
-------------------
The day is divided into six time slots:
- 00:00–06:59
- 07:00–09:59
- 10:00–13:59
- 14:00–16:59
- 17:00–19:59
- 20:00–23:59

Each weekday has a predefined list of entry probabilities for each of these slots.

Trip Duration Model
-------------------
Customer visit durations are sampled according to the following distribution:
- 35% of customers stay exactly 30 minutes
- 39% of customers stay between 31 and 44 minutes
- 26% of customers stay between 45 and 75 minutes

Data Source
-----------
All probabilities and assumptions are based on data from the following report:

"The State of Grocery Shopping: An Analysis of 1,000 U.S. Grocery Shoppers"
Published by Drive Research
Available at:
https://www.driveresearch.com/market-research-company-blog/grocery-store-statistics-where-when-how-much-people-grocery-shop/

This report summarizes behaviors of over 1,000 U.S. grocery shoppers, including average shopping frequency, peak hours, and visit duration.

Usage (Docker Compose)
----------------------
To build the producer container:

    docker-compose build kafka-producer-traffic

To run the container:

    docker-compose up -d kafka-producer-traffic

To view real-time logs:

    docker-compose logs -f kafka-producer-traffic
