import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { FormBuilder, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { TestingMngService } from './data-access/server-management.service';
import { WebSocketService } from '../../core/websocket.service';
import { AsyncPipe, NgClass, TitleCasePipe } from '@angular/common';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatIconModule } from '@angular/material/icon';
@Component({
  selector: 'app-server-management-panel',
  templateUrl: './server-management.component.html',
  styleUrls: ['./server-management.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports:[
    AsyncPipe,
    NgClass,
    TitleCasePipe,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatIconModule,
    ReactiveFormsModule
  ],
  providers:[
    WebSocketService,
    TestingMngService
  ],
})
export class ServerManagementPanelComponent {
  public wssCore = inject(WebSocketService);
  private testingService = inject(TestingMngService);
  public manageStreamForm: FormGroup = inject(FormBuilder).group({
      cmd: ['start'],
      timeToWork: [30, { validators: [Validators.required, Validators.pattern('[0-9]*')] }],
      intervalToEmit: [50, { validators: [Validators.required, Validators.pattern('[0-9]*')] }],
      market: ['n'],
    });
  manageStream() {
    const cmd = this.wssCore.streamActive ? 'stop' : 'start';
    this.manageStreamForm.get('cmd')?.patchValue(cmd);
    this.testingService.sendMessageToServer(this.manageStreamForm.value);
  }
  get interval() {
    return this.manageStreamForm.get('intervalToEmit');
  }
  get timeToWork() {
    return this.manageStreamForm.get('timeToWork');
  }
}
