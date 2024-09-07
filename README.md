# TravelTide-ProjectStep 1: Filter the sessions data
WITH FilteredSessions AS (
  SELECT
    user_id,
    session_id,
    session_start,
    session_end,
    page_clicks,
    flight_discount,
    hotel_discount,
    flight_booked,
    hotel_booked,
    cancellation,
    trip_id
  FROM sessions
  WHERE session_start >= '2023-01-04'  -- Filter sessions starting from January 4, 2023
),

-- Step 2: Aggregate the filtered sessions data
AggregatedSessions AS (
  SELECT
    user_id,
    COUNT(DISTINCT session_id) AS num_sessions, -- Count the number of unique sessions per user
    SUM(page_clicks) AS total_page_clicks, -- Total page clicks across all sessions
    SUM(CASE WHEN flight_discount THEN 1 ELSE 0 END) AS total_flight_discount, -- Count sessions with flight discounts
    SUM(CASE WHEN hotel_discount THEN 1 ELSE 0 END) AS total_hotel_discount, -- Count sessions with hotel discounts
    SUM(CASE WHEN flight_booked THEN 1 ELSE 0 END) AS total_flights_booked, -- Count sessions where a flight was booked
    SUM(CASE WHEN hotel_booked THEN 1 ELSE 0 END) AS total_hotels_booked, -- Count sessions where a hotel was booked
    SUM(CASE WHEN cancellation THEN 1 ELSE 0 END) AS total_cancellations, -- Count sessions where a booking was canceled
    AVG(COALESCE(f.checked_bags, 0)) AS avg_bags, -- Average number of checked bags per user
    COUNT(CASE WHEN f.checked_bags > 0 THEN 1 END) * 1.0 / COUNT(DISTINCT session_id) AS combined_booking_rate -- Ratio of sessions with bookings including checked bags
  FROM FilteredSessions fs
  LEFT JOIN flights f ON fs.trip_id = f.trip_id -- Join with flights data
  GROUP BY user_id
  HAVING COUNT(DISTINCT session_id) > 7 -- Only include users with more than 7 sessions
),

-- Step 3: Aggregate flight data
FlightsData AS (
  SELECT
    fs.user_id,
    SUM(f.checked_bags) AS total_checked_bags, -- Total number of checked bags
    MAX(f.destination_airport_lat) AS destination_airport_lat, -- Maximum latitude of destination airports
    MAX(f.destination_airport_lon) AS destination_airport_lon, -- Maximum longitude of destination airports
    MAX(f.departure_time) AS departure_time,  -- Most recent departure time
    MAX(f.return_time) AS return_time, -- Most recent return time
    SUM(f.base_fare_usd) AS total_flight_spent, -- Total amount spent on flights
    SUM(f.base_fare_usd) AS base_fare_usd -- Total base fare for flights
  FROM FilteredSessions fs
  JOIN flights f ON fs.trip_id = f.trip_id -- Join with flights data
  GROUP BY fs.user_id
),

-- Step 4: Aggregate hotel data
HotelsData AS (
  SELECT
    fs.user_id,
    SUM(h.hotel_per_room_usd) AS total_hotel_spent, -- Total amount spent on hotels
    SUM(h.hotel_per_room_usd) AS hotel_per_room_usd -- Total room cost for hotels
  FROM FilteredSessions fs
  JOIN hotels h ON fs.trip_id = h.trip_id -- Join with hotels data
  GROUP BY fs.user_id
),

-- Step 5: Determine perk eligibility
PerkEligibility AS (
  SELECT
    a.user_id,

    -- Determine loyalty perk based on bookings
    CASE
      WHEN a.total_flights_booked > 10 OR a.total_hotels_booked > 5 THEN 'Platinum'
      WHEN a.total_flights_booked > 5 OR a.total_hotels_booked > 3 THEN 'Gold'
      WHEN a.total_flights_booked >= 1 AND a.total_hotels_booked >= 1 THEN 'Silver'
      ELSE 'Basic'
    END AS loyalty_perk,

    -- Determine other perks based on specific conditions
    CASE
      WHEN a.total_cancellations = 0 AND a.total_flights_booked + a.total_hotels_booked >= 8 THEN 'No Cancellation Fees'
      WHEN a.total_hotels_booked >= 1 AND COALESCE(hd.total_hotel_spent, 0) >= 500 THEN 'Free Hotel Meal'
      WHEN a.total_flights_booked >= 2 AND COALESCE(fd.total_checked_bags, 0) >= 2 THEN 'Free Checked Bags'
      WHEN a.total_flights_booked >= 1 AND a.total_hotels_booked >= 2 THEN 'One Night Free Hotel with Flight'
      WHEN COALESCE(fd.total_flight_spent, 0) + COALESCE(hd.total_hotel_spent, 0) >= 100 THEN 'Exclusive Discount'
      ELSE 'Basic Perk'
    END AS perk_name

  FROM AggregatedSessions a
  LEFT JOIN FlightsData fd ON a.user_id = fd.user_id
  LEFT JOIN HotelsData hd ON a.user_id = hd.user_id
),

-- Step 6: Identify the most recent session
LatestSession AS (
  SELECT
    user_id,
    MAX(session_start) AS latest_session_start, -- Latest session start time
    MAX(session_end) AS latest_session_end -- Latest session end time
  FROM FilteredSessions
  GROUP BY user_id
),

-- Step 7: Calculate total seats booked by user
TotalSeats AS (
  SELECT
    fs.user_id,
    SUM(f.seats) AS total_seats -- Total number of seats booked by the user
  FROM FilteredSessions fs
  JOIN flights f ON fs.trip_id = f.trip_id -- Join with flights data
  GROUP BY fs.user_id
),

-- Step 8: Calculate total rooms booked by user
TotalRooms AS (
  SELECT
    fs.user_id,
    SUM(h.rooms) AS total_rooms -- Total number of rooms booked by the user
  FROM FilteredSessions fs
  JOIN hotels h ON fs.trip_id = h.trip_id -- Join with hotels data
  GROUP BY fs.user_id
),

-- Step 9: Scale average bags data
AvgBagsScaled AS (
  SELECT
    user_id,
    (avg_bags - MIN(avg_bags) OVER()) / NULLIF((MAX(avg_bags) OVER() - MIN(avg_bags) OVER()), 0) AS avg_bags_scaled -- Scale average bags data
  FROM AggregatedSessions
),

-- Step 10: Calculate bargain hunter index
BargainHunterIndex AS (
  SELECT
    user_id,
    (total_flight_discount * 1.0 / num_sessions) AS bargain_hunter_index -- Calculate bargain hunter index based on discounts
  FROM AggregatedSessions
),

-- Step 11: Calculate hotel hunter index
HotelHunterIndex AS (
  SELECT
    user_id,
    (total_hotels_booked * 1.0 / num_sessions) * (total_hotel_discount * 1.0 / num_sessions) AS hotel_hunter_index -- Calculate hotel hunter index based on bookings and discounts
  FROM AggregatedSessions
)

-- Step 12: Final query to select and join all relevant data
SELECT
  u.user_id,
  DATE_PART('YEAR', AGE(CURRENT_DATE, u.birthdate)) AS age, -- Calculate user's age
  CASE
    WHEN DATE_PART('YEAR', AGE(CURRENT_DATE, u.birthdate)) < 18 THEN 'Under 18'
    WHEN DATE_PART('YEAR', AGE(CURRENT_DATE, u.birthdate)) BETWEEN 18 AND 34 THEN '18-34'
    WHEN DATE_PART('YEAR', AGE(CURRENT_DATE, u.birthdate)) BETWEEN 35 AND 49 THEN '35-49'
    WHEN DATE_PART('YEAR', AGE(CURRENT_DATE, u.birthdate)) BETWEEN 50 AND 64 THEN '50-64'
    ELSE '65+'
  END AS age_group, -- Categorize age groups
  u.gender,
  u.married,
  u.has_children,
  u.home_country,
  u.home_city,
  u.home_airport,
  u.sign_up_date,
  a.num_sessions,
  a.total_page_clicks,
  a.total_flight_discount,
  a.total_hotel_discount,
  a.total_flights_booked,
  a.total_hotels_booked,
  a.total_cancellations,
  fd.total_checked_bags,
  fd.destination_airport_lat,
  fd.destination_airport_lon,
  u.home_airport_lat,
  u.home_airport_lon,
  ts.total_seats,
  tr.total_rooms,
  fd.departure_time,
  fd.return_time,
  ls.latest_session_start,
  ls.latest_session_end,
  pe.perk_name,
  pe.loyalty_perk,
  fd.base_fare_usd,
  hd.hotel_per_room_usd,
  ab.avg_bags_scaled,
  a.total_cancellations * 1.0 / a.num_sessions AS cancelation_rate_scale, -- Calculate scaled cancellation rate
  bhi.bargain_hunter_index,
  a.combined_booking_rate,
  hhi.hotel_hunter_index,

  (fd.total_flight_spent * 1.0 + COALESCE(hd.total_hotel_spent, 0)) / a.num_sessions AS avg_spend_per_session, -- Calculate average spend per session
  (fd.total_flight_spent * 1.0 + COALESCE(hd.total_hotel_spent, 0)) AS total_spend, -- Calculate total spend
  EXTRACT(EPOCH FROM (ls.latest_session_end - ls.latest_session_start)) / 3600 AS session_duration_hours, -- Calculate session duration in hours
  RANK() OVER (ORDER BY ab.avg_bags_scaled DESC) AS rank_avg_bags, -- Rank users by average bags scaled
  RANK() OVER (ORDER BY bhi.bargain_hunter_index DESC) AS rank_bargain_hunter, -- Rank users by bargain hunter index
  RANK() OVER (ORDER BY a.total_cancellations * 1.0 / a.num_sessions ASC) AS rank_cancelation_rate, -- Rank users by cancellation rate
  RANK() OVER (ORDER BY a.combined_booking_rate DESC) AS rank_combined_booking_rate, -- Rank users by combined booking rate
  RANK() OVER (ORDER BY hhi.hotel_hunter_index DESC) AS rank_hotel_hunter, -- Rank users by hotel hunter index
  RANK() OVER (ORDER BY a.total_page_clicks DESC) AS rank_session_activity, -- Rank users by session activity
  a.total_page_clicks * 1.0 / a.num_sessions AS session_intensity_index -- Calculate session intensity index
FROM users u
INNER JOIN AggregatedSessions a ON u.user_id = a.user_id
LEFT JOIN FlightsData fd ON u.user_id = fd.user_id
LEFT JOIN HotelsData hd ON u.user_id = hd.user_id
LEFT JOIN PerkEligibility pe ON u.user_id = pe.user_id
LEFT JOIN LatestSession ls ON u.user_id = ls.user_id
LEFT JOIN TotalSeats ts ON u.user_id = ts.user_id
LEFT JOIN TotalRooms tr ON u.user_id = tr.user_id
LEFT JOIN AvgBagsScaled ab ON u.user_id = ab.user_id
LEFT JOIN BargainHunterIndex bhi ON u.user_id = bhi.user_id
LEFT JOIN HotelHunterIndex hhi ON u.user_id = hhi.user_id
ORDER BY u.user_id; -- Order results by user ID
