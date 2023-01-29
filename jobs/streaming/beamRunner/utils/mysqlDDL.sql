--DROP DATABASE IF EXISTS `ikea_usecase`;
CREATE DATABASE IF NOT EXISTS `ikea_usecase`;
USE `ikea_usecase`;

-- Update the Character set and collate variables in the DB so we can store the emojis
SET NAMES utf8mb4;
ALTER DATABASE ikea_usecase CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci;

-- Create users table if it is not present
CREATE TABLE IF NOT EXISTS `tweets_users` (
  `id` varchar(50) NOT NULL,
  `name` varchar(50),
  `screen_name` varchar(50),
  `location` text,
  `description` text,
  `protected` tinyint(1),
  `followers_count` float,
  `listed_count` float,
  `created_at` varchar(50),
  `favourites_count` float,
  `geo_enabled` tinyint(1),
  `verified` tinyint(1),
  `lang` varchar(50),
  `has_extended_profile` tinyint(1),
  PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4;

-- Create tweets table if it is not present
CREATE TABLE IF NOT EXISTS `tweets` (
  `id` varchar(50) NOT NULL,
  `created_at` varchar(50),
  `retweet_count` float,
  `favorite_count` float,
  `favorited` tinyint(1),
  `retweeted` tinyint(1),
  `lang` varchar(50),
  `full_text` text,
  `user_id` varchar(50) NOT NULL,
  PRIMARY KEY (`id`)
) DEFAULT CHARACTER SET utf8mb4;

-- Create tweets entities table if it is not present
CREATE TABLE IF NOT EXISTS `tweets_entities` (
  `tweet_id` varchar(50) NOT NULL,
  `entitiesData` JSON,
  PRIMARY KEY (`tweet_id`)
) DEFAULT CHARACTER SET utf8mb4;

-- Create tweets translations table if it is not present
CREATE TABLE IF NOT EXISTS `tweets_translations` (
  `tweet_id` varchar(50) NOT NULL,
  `originalText` text,
  `originalLang` varchar(50),
  `translatedText` text,
  `translatedLang` varchar(50),
  PRIMARY KEY (`tweet_id`)
) DEFAULT CHARACTER SET utf8mb4;

