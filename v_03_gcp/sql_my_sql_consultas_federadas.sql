CREATE DATABASE caio_db;

USE caio_db;

CREATE TABLE IF NOT EXISTS cliente (
	cliente_id INT PRIMARY KEY,
	nome varchar(255) NOT NULL,
	data_nasc DATE,
	cpf VARCHAR(255) NOT NULL
) ;

INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('739','Caio Falco','1994/03/29','022.628.645-27');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('273','Jose da Silva','1990/09/30','039.118.910-54');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('2091','Maria Alves','1985/01/25','431.889.153-12');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('1737','Leandro Pereira Santos','1982/07/25','871.044.549-06');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('542','Lizandro Ferreira Paiva','1992/12/25','643.124.198-20');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('1292','Leonardo Guedes Mazzero','1988/11/10','677.521.080-90');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('881','Guilherme Balog','1980/04/12','188.515.620-08');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('2388','Pedro Xavier','1971/05/01','569.430.470-51');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('2098','Luisa Almeida Galvão','1962/08/12','310.693.420-41');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('991','Gilmar Camara','1968/10/07','796.135.750-57');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('783','Ariel Leitão Prudente','1988/11/01','190.035.830-13');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('440','Letícia Valim Ouro','1981/01/31','621.110.290-92');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('791','Eliseu Guterres Espinosa','1984/12/05','673.374.060-53');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('1598','Lázaro Flávio Valim','1991/02/21','959.740.840-63');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('1487','Arsénio Roçadas Garcia',NULL,'792.231.890-12');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('931','Lian Barreno Orriça','1959/08/13','651.097.020-12');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('609','Hossana Vilanova Portela','1989/07/08','159.088.580-56');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('1994','Pavel Belmonte Cabeça','1990/02/11','923.689.800-91');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('1071','César Cirne Camilo',NULL,'099.397.730-80');
INSERT INTO cliente (cliente_id, nome, data_nasc, cpf)
VALUES ('891','Eshal Vasques Queirós','1977/11/28','126.077.500-32');
