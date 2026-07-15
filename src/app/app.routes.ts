import { RouterModule, Routes } from '@angular/router';
import { RTQuotesTableComponent } from './components/rt-quotes-table/rt-quotes-table.component';
import { NgModule } from '@angular/core';

export const appRoutes: Routes  = [
  {
    path:'',
    component:RTQuotesTableComponent,
  }
];
@NgModule ({
  imports:[RouterModule.forRoot(appRoutes)],
  exports:[RouterModule]
})
export class AppRouteModule {}