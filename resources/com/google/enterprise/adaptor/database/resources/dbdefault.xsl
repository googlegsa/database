<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<!-- This stylesheet can be downloaded from:
  https://storage.googleapis.com/support-kms-prod/SNP_894EFA432F67D7FDD40584D3A884338D6B3D_6069367_en_v0
  -->

<xsl:output method="html" encoding="UTF-8" indent="yes"/>

<!-- **********************************************************************
 Render header and footer
     ********************************************************************** -->
<xsl:template match="/">
 <html>
  <head>
   <title>Default Page</title>
  </head>
  <body style="font-family: arial, helvetica, sans-serif;">
   <H2 align="center">Database Result</H2>
   <xsl:apply-templates />
  </body>
 </html>
</xsl:template>

<!-- **********************************************************************
 Render the database result
     ********************************************************************** -->
<xsl:template match="/database">
 <xsl:if test="/database/RES">
  <xsl:variable name="count" select="count(/database/table/table_rec)"/>
  <xsl:variable name="view_begin" select="/database/RES/@SN"/>
  <xsl:variable name="view_end" select="/database/RES/@EN"/>
  <xsl:variable name="guess" select="/database/RES/M"/>
  <xsl:variable name="query">
  <xsl:value-of disable-output-escaping="yes" 
   select="normalize-space(/database/PARAM[@name='q']/@value)"/>
  </xsl:variable>
  <table width="100%" border="0" cellpadding="2" 
         cellspacing="0" bgcolor="#e5ecf9">
   <tr>
    <td bgcolor="#e5ecf9" nowrap="true">
    <xsl:if test="$query != ''"><font color="#000000" size="-1">
     searched for <b><xsl:value-of select="$query"/></b>.</font>
    </xsl:if>
    </td>
    <td bgcolor="#e5ecf9" align="right" nowrap="true">
    <xsl:if test="$query != ''"><font color="#000000" size="-1">
     Result <b><xsl:value-of select="$view_begin"/></b> - 
       <b><xsl:value-of select="$view_begin + $count - 1"/></b> of about 
       <b><xsl:value-of select="$guess"/></b>.</font>
    </xsl:if>
    </td>
   </tr>
  </table>
 </xsl:if>
 <xsl:apply-templates />
</xsl:template>

<!-- **********************************************************************
 Render the table
     ********************************************************************** -->
<xsl:template match="/database/table">
 <table border="0" width="100%" align="center" cellspacing="0" cellpadding="3">
  <!-- Table header -->
  <tr>
   <xsl:for-each select="table_rec[1]/*">
    <xsl:call-template name="column_header"/>
   </xsl:for-each>
  </tr>
  <xsl:apply-templates />
 </table>
</xsl:template>

<!-- **********************************************************************
 Render the table
     ********************************************************************** -->
<xsl:template name="column_header" xml:space="preserve">
 <th align="left" style="border-bottom: #ffcc00 1px solid;">
  <xsl:value-of select="name(.)" />
 </th>
</xsl:template>

<!-- **********************************************************************
 Render each record
     ********************************************************************** -->
<xsl:template match="/database/table/table_rec">
 <xsl:variable name="bgcolor">
  <xsl:choose>
   <xsl:when test="position() mod 2 = 0">F5F5F5</xsl:when>
   <xsl:otherwise>white</xsl:otherwise>
  </xsl:choose>
 </xsl:variable>
 <tr bgcolor="{$bgcolor}" valign="top">
  <xsl:apply-templates />
 </tr>
</xsl:template>

<!-- **********************************************************************
 Render each field of each record
     ********************************************************************** -->
<xsl:template match="/database/table/table_rec/*" xml:space="preserve">
 <td><font size="-1"><xsl:value-of select="text()"/></font></td>
</xsl:template>

<!-- **********************************************************************
 Render result navigation links
     ********************************************************************** -->

<xsl:template match="/database/RES">
 <xsl:variable name="prev" select="/database/RES/NB/PU"/>
 <xsl:variable name="next" select="/database/RES/NB/NU"/>
 <xsl:variable name="view_begin" select="/database/RES/@SN"/>
 <xsl:variable name="view_end" select="/database/RES/@EN"/>
 <xsl:variable name="guess" select="/database/RES/M"/>
 <table width="100%" align="center">
  <tr>
   <td colspan="10" align="center" bgcolor="#e5ecf9">
    <font size="-1"><b>
     <xsl:if test="$prev or $next"> more results: </xsl:if>
     <xsl:if test="$prev"> 
      <a href="{$prev}">&#171; previous</a> 
     </xsl:if>
     <xsl:if test="$next">
      <xsl:if test="$prev"> | </xsl:if> 
      <a href="{$next}">next &#187;</a> 
     </xsl:if>
    </b></font>
   </td>
  </tr>
 </table>
</xsl:template>

<!-- **********************************************************************
 Swallow unmatched elements
     ********************************************************************** -->
<xsl:template match="@*|node()"/>

</xsl:stylesheet>
