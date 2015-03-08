create table dagar(
	dag date primary key,
	variant smallint not null
);

create table leidir(
	lid int primary key,
	num text not null,
	leid text not null
);

create table stodvar(
	stod int primary key,
	lon double precision not null,
	lat double precision not null,
	nafn text not null
);

create table ferdir(
	id serial primary key,
	lid int references leidir(lid),
	variant int not null,
	start time not null,
	stop time not null
);

create table stops(
	ferd int references ferdir(id),
	stod int references stodvar(stod),
	timi time not null,
	stnum int not null
);
