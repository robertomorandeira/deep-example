-- ############# INSERT STATEMENTS #############

insert into football.team (name, short_name, arena_name, coach_name, city_name, league_name) values
('FC Bayern München', 'FCB', 'Allianz Arena', 'Josep Guardiola', 'München', 'Bundesliga'),
('Hamburger SV', 'HSV', 'Imtech Arena', 'Josef Zinnbauer', 'Hamburg', 'Bundesliga'),
('Herta BSC Berlin', 'Herta', 'Olympiastaion Berlin', 'Jos Luhukay', 'Berlin', 'Bundesliga'),
('FC Basel 1893', 'FCB', 'St. Jakob-Park', 'Paulo Sousa', 'Basel', 'Raiffeisen Super League'),
('FC Paris Saint-Germain', 'PSG', 'Parc des Princes', 'Laurent Blanc', 'Paris', 'Ligue 1'),
('HJK Helsinki', 'HJK', 'Sonera Stadium', 'Mika Lehkosuo', 'Helsinki', 'Veikkausliiga');

insert into football.player(firstname, lastname, date_of_birth, place_of_birth_name, position_name, team_id) values
('Manuel', 'Neuer', '1986-3-27', 'Gelsenkirchen', 'Goalkeeper', 1),
('Julian', 'Schieber', '1989-2-13', 'Backnang', 'Centre Forward', 3),
('Dennis ', 'Diekmeier', '1989-10-20', 'Thedinghausen', 'Right Wing', 2),
('Zlatan', 'Ibrahimovic', '1981-10-03', 'Malmö', 'Centre Forward', 5),
('Xabier', 'Alonso', '1981-11-25', 'Tolosa', 'Midfielder', 1);