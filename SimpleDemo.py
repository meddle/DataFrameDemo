# Databricks notebook source
# MAGIC %sql
# MAGIC select color,sum(carat) as tcarats from diamonds group by color order by tcarats