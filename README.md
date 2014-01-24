fintp_connectors
================

Enables the FinTP server to communicate with other systems and applications	

Requirements
------------
- Boost
- Xerces-C
- Xalan-C
- ZipArchive
- Libtiff
- pthread
- **fintp_utils**
- **fintp_log**
- **fintp_transport**
- **fintp_udal**
- **fintp_base**

Build instructions
------------------
- On Unix-like systems, **fintp_connectors** uses the GNU Build System (Autotools) so we first need to generate the configuration script using:


        autoreconf -fi
Now we must run:

        ./configure [--with-udal]
        make
By default, a MQ connector is built. Using **--with-udal** will build a database connector.
- For Windows, a Visual Studio 2010 solution is provided.

License
-------
- [GPLv3](http://www.gnu.org/licenses/gpl-3.0.html)

