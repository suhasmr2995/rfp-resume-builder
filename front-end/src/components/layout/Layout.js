import React from 'react';
import Sidebar from './Sidebar';
import Header from './Header';

const Layout = ({ children, pages, activePage, setActivePage }) => {
  return (
    <>
      <div className="min-h-screen h-full flex flex-col flex-1 justify-start bg-gradient-to-br from-gray-900 to-gray-800 overflow-auto">
        <div>
          <Header />
        </div>
        <div className="flex min-h-screen w-full h-full bg-gradient-to-br from-gray-900 to-gray-800 text-white">
          <Sidebar pages={pages} activePage={activePage} setActivePage={setActivePage} />
          <div className="flex-1 p-10">
            {children}
          </div>
        </div>
      </div>
    </>

  );
};

export default Layout;
