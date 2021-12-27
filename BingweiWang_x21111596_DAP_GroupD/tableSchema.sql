CREATE TABLE `td_order_category_quantity` (
  `category` varchar(255) NOT NULL DEFAULT '' COMMENT 'Category',
  `quantity` int(11) NOT NULL DEFAULT '0' COMMENT 'Quantity',
  PRIMARY KEY (`category`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='Category Purchase Quantity Table';

CREATE TABLE `td_order_country_sales` (
  `country` varchar(255) NOT NULL DEFAULT '' COMMENT 'Country',
  `salesAmount` double NOT NULL DEFAULT '0' COMMENT 'Sales',
  PRIMARY KEY (`country`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='Sales tables';

CREATE TABLE `td_order_predict_buy` (
  `cnt` int(11) NOT NULL COMMENT 'Shoping num',
  `intervals` int(11) NOT NULL DEFAULT '0' COMMENT 'Predict interval',
  PRIMARY KEY (`cnt`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;