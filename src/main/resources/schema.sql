-- ############# DDL #############

-- Simplified schema of that one: https://github.com/footballdata/sport_db.Football/

create schema football;

create table if not exists football.team
(
	id bigint not null auto_increment,
	name varchar(255) not null,
	short_name varchar(255) not null,
	arena_name varchar(255) not null,
	coach_name varchar(255) not null,
	city_name varchar(255) not null,
	league_name varchar(255) not null,
	primary key (id)
);

create table if not exists football.player
(
	id bigint not null auto_increment,
	firstname varchar(255) not null,
	lastname varchar(255) not null,
	date_of_birth date,
	place_of_birth_name varchar(255) not null,
	position_name varchar(255) not null,
    team_id bigint not null,
	primary key (id),
	foreign key (team_id) references football.team(id)
);