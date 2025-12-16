CREATE PUBLICATION bankcdc_pub FOR TABLE
  bank.transaction,
  bank.card;